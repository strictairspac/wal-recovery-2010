from datetime import datetime, timedelta, timezone
from sqlalchemy import and_, or_, select, update
from fastapi import HTTPException
from src.config.settings import RECOVERY_LEASE_SECONDS
from src.constants.enums import PENDING_STATUSES
from src.models.entities import Recovery, CrashCase, LogSet, RecoveryEvent
from src.utils.serialize import record

LEASE_SECONDS = RECOVERY_LEASE_SECONDS
STATUS_LABELS = {'queued': '等待执行', 'running': '执行中', 'completed': '已完成', 'failed': '失败', 'cancelled': '已取消'}
CLAIM_REASON_QUEUE = '入队后由执行器领取'
CLAIM_REASON_TAKEOVER = '执行者失联，租约过期后自动接管'
CANCEL_REASON_DEFAULT = '值班人员手动取消'


def _now(): return datetime.now(timezone.utc).replace(tzinfo=None)


def list_recoveries(db): return [record(x) for x in db.scalars(select(Recovery).order_by(Recovery.id.desc()).limit(100))]


def get_recovery(db, ident):
    obj = db.get(Recovery, ident)
    if obj is None: raise HTTPException(404, '恢复记录不存在')
    return obj


def list_events(db, ident):
    return [record(e) for e in db.scalars(select(RecoveryEvent).where(RecoveryEvent.recovery_id == ident).order_by(RecoveryEvent.id))]


def detail_payload(db, obj):
    payload = record(obj)
    payload['events'] = list_events(db, obj.id)
    return payload


def pending_count(db):
    return len(db.scalars(select(Recovery.id).where(Recovery.status.in_(PENDING_STATUSES))).all())


def create_recovery(db, data, actor='system'):
    case = db.get(CrashCase, data.crash_case_id)
    if case is None: raise HTTPException(404, '故障场景不存在')
    if pending_count(db) >= 20: raise HTTPException(429, '待处理配额20已满')
    logs = db.get(LogSet, case.log_set_id)
    # 冻结输入只在此处生成一次，后续取消、接管、完成都不修改 snapshot。
    obj = Recovery(**data.model_dump(), status='queued', snapshot={'log_set': record(logs), 'case': record(case)})
    db.add(obj); db.flush()
    db.add(RecoveryEvent(recovery_id=obj.id, event='enqueued', actor=actor, reason='恢复任务已入队'))
    return obj


def cancel_recovery(db, ident, actor='system', reason=''):
    """Cancel queued or running work with a single conditional update.

    The request returns immediately and never waits for the computation. A running
    worker can only publish through finish_recovery(), whose predicate requires
    status='running', so a confirmed cancel prevents any later result publication.
    """
    obj = get_recovery(db, ident)
    reason = (reason or CANCEL_REASON_DEFAULT).strip()[:300]
    result = db.execute(update(Recovery).where(Recovery.id == ident, Recovery.status.in_(PENDING_STATUSES))
                        .values(status='cancelled', finished_at=_now(), cancel_reason=reason, lease_expires_at=None))
    changed = result.rowcount == 1
    if changed:
        db.add(RecoveryEvent(recovery_id=ident, event='cancelled', actor=actor, reason=reason))
    db.flush(); db.refresh(obj)
    if obj.status == 'cancelled':
        return obj, changed
    raise HTTPException(409, f'任务{STATUS_LABELS.get(obj.status, obj.status)}，无法取消')


def claim_next(db, owner, lease_seconds=LEASE_SECONDS):
    """Claim the oldest queued job, or take over a job whose lease has expired.

    The claim is a single conditional UPDATE compared against the observed
    (status, attempt) pair, so two executors can never own one attempt. Every
    claim increments `attempt`; that value is the fencing token checked by
    renew_lease()/finish_recovery(). A returning executor from an older attempt
    can therefore never overwrite the takeover result.
    """
    for _ in range(5):
        now_ = _now()
        expired = or_(Recovery.lease_expires_at.is_(None), Recovery.lease_expires_at < now_)
        candidate = db.execute(select(Recovery.id, Recovery.status, Recovery.attempt)
                               .where(or_(Recovery.status == 'queued', and_(Recovery.status == 'running', expired)))
                               .order_by(Recovery.id).limit(1)).first()
        if candidate is None: return None
        ident, observed_status, observed_attempt = candidate
        takeover = observed_status == 'running'
        attempt = (observed_attempt or 0) + 1
        attempt_guard = Recovery.attempt.is_(None) if observed_attempt is None else Recovery.attempt == observed_attempt
        claim_reason = CLAIM_REASON_TAKEOVER if takeover else CLAIM_REASON_QUEUE
        claimed = db.execute(update(Recovery).where(Recovery.id == ident, Recovery.status == observed_status, attempt_guard)
                             .values(status='running', attempt=attempt, owner=owner, claimed_at=now_,
                                     heartbeat_at=now_, lease_expires_at=now_ + timedelta(seconds=lease_seconds),
                                     claim_reason=claim_reason))
        if claimed.rowcount != 1:
            db.rollback()
            continue
        db.add(RecoveryEvent(recovery_id=ident, event='takeover' if takeover else 'claimed', actor=owner, reason=claim_reason))
        db.commit()
        return db.get(Recovery, ident)
    return None


def renew_lease(db, ident, owner, attempt, lease_seconds=LEASE_SECONDS):
    now_ = _now()
    result = db.execute(update(Recovery).where(Recovery.id == ident, Recovery.status == 'running', Recovery.owner == owner, Recovery.attempt == attempt)
                        .values(heartbeat_at=now_, lease_expires_at=now_ + timedelta(seconds=lease_seconds)))
    db.commit()
    return result.rowcount == 1


def finish_recovery(db, ident, owner, attempt, status, result):
    """Publish result and terminal status atomically, guarded by fencing token.

    Returns (outcome, obj): applied / already_applied / rejected / missing.
    already_applied makes an ambiguous commit retry idempotent; it never writes a
    duplicate terminal event or a second result.
    """
    if status not in ('completed', 'failed'): raise ValueError('终态只能是 completed 或 failed')
    now_ = _now()
    updated = db.execute(update(Recovery).where(Recovery.id == ident, Recovery.status == 'running', Recovery.owner == owner, Recovery.attempt == attempt)
                         .values(result=result, status=status, finished_at=now_, heartbeat_at=now_, lease_expires_at=None))
    if updated.rowcount == 1:
        db.add(RecoveryEvent(recovery_id=ident, event=status, actor=owner,
                             reason='恢复计算完成，结果与状态一同生效' if status == 'completed' else f'恢复计算失败：{result.get("error", "未知错误")}'))
        db.commit()
        obj = db.get(Recovery, ident); db.refresh(obj)
        return 'applied', obj
    db.rollback()
    obj = db.get(Recovery, ident)
    if obj is None: return 'missing', None
    db.refresh(obj)
    if obj.status == status and obj.owner == owner and obj.attempt == attempt:
        return 'already_applied', obj
    reason = f'第{attempt}次领取的结果已丢弃：任务当前为{STATUS_LABELS.get(obj.status, obj.status)}（租约第{obj.attempt}次）'
    exists = db.scalar(select(RecoveryEvent.id).where(RecoveryEvent.recovery_id == ident, RecoveryEvent.event == 'discarded', RecoveryEvent.reason == reason))
    if not exists:
        db.add(RecoveryEvent(recovery_id=ident, event='discarded', actor=owner, reason=reason)); db.commit(); db.refresh(obj)
    return 'rejected', obj
