import threading
import time

import pytest
from fastapi import HTTPException
from sqlalchemy import select, update

from src.models.entities import Recovery, RecoveryEvent
from src.schemas.recovery import RecoveryInput
from src.services import recovery_service as svc
from src.services.wal_engine import recover
from src.config.database import Session
from src.worker import claim_and_run
from tests.conftest import auth


def make_job(case_id, label='晚班恢复核对'):
    with Session() as db:
        job = svc.create_recovery(db, RecoveryInput(crash_case_id=case_id, label=label), actor='operator@haili.local')
        db.commit()
        return job.id


def get(job_id):
    with Session() as db:
        return svc.detail_payload(db, svc.get_recovery(db, job_id))


def events(job_id, name=None):
    return [e for e in get(job_id)['events'] if name is None or e['event'] == name]


def expire_lease(job_id):
    with Session() as db:
        db.execute(update(Recovery).where(Recovery.id == job_id).values(lease_expires_at=svc._now().replace(year=2000)))
        db.commit()


# --- 取消 -----------------------------------------------------------------

@pytest.mark.parametrize('state', ['queued', 'running'])
def test_cancel_queued_and_running_returns_without_waiting(case_id, state):
    job_id = make_job(case_id)
    if state == 'running':
        with Session() as db:
            assert svc.claim_next(db, 'executor-a').id == job_id

    started = time.monotonic()
    with Session() as db:
        obj, changed = svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()
    assert time.monotonic() - started < 1.0
    assert changed is True
    assert obj.status == 'cancelled'
    assert obj.result is None
    assert obj.cancel_reason == svc.CANCEL_REASON_DEFAULT
    assert obj.lease_expires_at is None
    assert obj.finished_at is not None
    kinds = [e['event'] for e in events(job_id)]
    assert kinds[-1] == 'cancelled'
    assert kinds.count('cancelled') == 1
    assert kinds[0] == 'enqueued'


def test_cancel_confirmed_prevents_late_result_publication(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        attempt = job.attempt
        svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()

    # 执行者算完后照常提交；确认过取消后不能发布结果。
    with Session() as db:
        outcome, obj = svc.finish_recovery(db, job_id, 'executor-a', attempt, 'completed',
                                           recover({'sample_A': 100, 'sample_B': 50},
                                                   get(job_id)['snapshot']['log_set']['records'], 10))
    assert outcome == 'rejected'
    stored = get(job_id)
    assert stored['status'] == 'cancelled'
    assert stored['result'] is None
    assert stored['finished_at'] == get(job_id)['finished_at']
    assert events(job_id, 'completed') == []
    assert len(events(job_id, 'discarded')) == 1
    assert '已取消' in events(job_id, 'discarded')[0]['reason']


def test_duplicate_cancel_does_not_duplicate_events(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        obj1, first = svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()
    with Session() as db:
        obj2, second = svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()
    assert (first, second) == (True, False)
    assert obj2.status == 'cancelled'
    assert len(events(job_id, 'cancelled')) == 1
    assert len(events(job_id)) == 2  # enqueued + cancelled


def test_cancel_terminal_job_is_rejected(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        svc.finish_recovery(db, job_id, 'executor-a', job.attempt, 'completed', {'invariant_passed': True})
    with Session() as db:
        with pytest.raises(HTTPException) as exc:
            svc.cancel_recovery(db, job_id, actor='operator@haili.local')
    assert exc.value.status_code == 409
    assert get(job_id)['result'] == {'invariant_passed': True}


def test_cancel_releases_pending_quota(case_id):
    ids = [make_job(case_id) for _ in range(20)]
    with Session() as db:
        with pytest.raises(HTTPException) as exc:
            svc.create_recovery(db, RecoveryInput(crash_case_id=case_id, label='超出配额'))
    assert exc.value.status_code == 429

    with Session() as db:
        svc.cancel_recovery(db, ids[0], actor='operator@haili.local')
        db.commit()
    with Session() as db:
        assert svc.pending_count(db) == 19
        obj = svc.create_recovery(db, RecoveryInput(crash_case_id=case_id, label='取消后补位'))
        db.commit()
    assert obj.status == 'queued'


# --- 失联接管 --------------------------------------------------------------

def test_takeover_after_lease_expiry_and_late_executor_cannot_overwrite(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        old = svc.claim_next(db, 'executor-a', lease_seconds=-1)  # 租约立即过期
        old_attempt = old.attempt
    with Session() as db:
        new = svc.claim_next(db, 'executor-b')
    assert new.id == job_id
    assert new.status == 'running'
    assert new.attempt == old_attempt + 1
    assert new.owner == 'executor-b'
    assert new.claim_reason == svc.CLAIM_REASON_TAKEOVER

    with Session() as db:
        outcome, _ = svc.finish_recovery(db, job_id, 'executor-b', new.attempt, 'completed', {'recovered': {'sample_A': 60}})
    assert outcome == 'applied'

    # 迟到的原执行者提交旧 attempt，必须被拒绝且不覆盖接管结果。
    with Session() as db:
        outcome, obj = svc.finish_recovery(db, job_id, 'executor-a', old_attempt, 'completed', {'recovered': {'sample_A': 0}})
    assert outcome == 'rejected'
    stored = get(job_id)
    assert stored['status'] == 'completed'
    assert stored['result'] == {'recovered': {'sample_A': 60}}
    assert stored['owner'] == 'executor-b'
    kinds = [e['event'] for e in stored['events']]
    assert kinds[:3] == ['enqueued', 'claimed', 'takeover']
    assert 'completed' in kinds
    assert 'discarded' in kinds
    assert len([k for k in kinds if k == 'completed']) == 1


def test_late_executor_cannot_resurrect_cancelled_takeover(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        old = svc.claim_next(db, 'executor-a', lease_seconds=-1)
        old_attempt = old.attempt
    with Session() as db:
        new = svc.claim_next(db, 'executor-b')
        svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()

    for owner, attempt in (('executor-a', old_attempt), ('executor-b', new.attempt)):
        with Session() as db:
            outcome, _ = svc.finish_recovery(db, job_id, owner, attempt, 'completed', {'recovered': {}})
        assert outcome == 'rejected'
    assert get(job_id)['result'] is None
    assert get(job_id)['status'] == 'cancelled'


def test_live_lease_is_not_taken_over(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        svc.claim_next(db, 'executor-a', lease_seconds=60)
    with Session() as db:
        assert svc.claim_next(db, 'executor-b') is None
    assert get(job_id)['owner'] == 'executor-a'


def test_renew_lease_is_fenced(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        old = svc.claim_next(db, 'executor-a', lease_seconds=-1)
        old_attempt = old.attempt
    expire_lease(job_id)
    with Session() as db:
        new = svc.claim_next(db, 'executor-b')
    with Session() as db:
        assert svc.renew_lease(db, job_id, 'executor-a', old_attempt) is False
        assert svc.renew_lease(db, job_id, 'executor-b', new.attempt) is True


# --- 完成 / 取消竞速 -------------------------------------------------------

def test_cancel_wins_race_against_completion(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        attempt = job.attempt
    # 取消先成功生效
    with Session() as db:
        svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()
    with Session() as db:
        outcome, _ = svc.finish_recovery(db, job_id, 'executor-a', attempt, 'completed', {'invariant_passed': True})
    assert outcome == 'rejected'
    assert get(job_id)['status'] == 'cancelled'
    with Session() as db:
        obj, changed = svc.cancel_recovery(db, job_id, actor='operator@haili.local')
    # 重复取消返回已有终态而不是覆盖
    assert changed is False
    assert obj.status == 'cancelled'
    assert len(events(job_id, 'cancelled')) == 1


def test_completion_wins_race_against_cancel(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        outcome, _ = svc.finish_recovery(db, job_id, 'executor-a', job.attempt, 'completed', {'invariant_passed': True})
    assert outcome == 'applied'
    with Session() as db:
        with pytest.raises(HTTPException) as exc:
            svc.cancel_recovery(db, job_id, actor='operator@haili.local')
    assert exc.value.status_code == 409
    stored = get(job_id)
    assert stored['status'] == 'completed'
    assert stored['result'] == {'invariant_passed': True}
    assert events(job_id, 'cancelled') == []


def test_concurrent_cancel_and_finish_have_single_terminal_state(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        attempt = job.attempt

    barrier = threading.Barrier(2)
    outcomes = {}

    def cancel_side():
        with Session() as db:
            barrier.wait()
            try:
                svc.cancel_recovery(db, job_id, actor='operator@haili.local'); db.commit()
                outcomes['cancel'] = 'applied'
            except HTTPException as exc:
                outcomes['cancel'] = exc.status_code

    def finish_side():
        with Session() as db:
            barrier.wait()
            outcome, _ = svc.finish_recovery(db, job_id, 'executor-a', attempt, 'completed', {'invariant_passed': True})
            outcomes['finish'] = outcome

    threads = [threading.Thread(target=cancel_side), threading.Thread(target=finish_side)]
    for t in threads: t.start()
    for t in threads: t.join()

    stored = get(job_id)
    terminal_events = [e for e in stored['events'] if e['event'] in ('completed', 'cancelled')]
    assert len(terminal_events) == 1, stored['events']
    if stored['status'] == 'cancelled':
        assert stored['result'] is None
        assert outcomes['finish'] == 'rejected'
    else:
        assert stored['status'] == 'completed'
        assert stored['result'] == {'invariant_passed': True}
        assert outcomes['cancel'] == 409


def test_retry_after_ambiguous_commit_is_idempotent(case_id):
    job_id = make_job(case_id)
    payload = recover({'sample_A': 100, 'sample_B': 50}, get(job_id)['snapshot']['log_set']['records'], 10)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        assert svc.finish_recovery(db, job_id, 'executor-a', job.attempt, 'completed', payload)[0] == 'applied'
    for _ in range(3):
        with Session() as db:
            assert svc.finish_recovery(db, job_id, 'executor-a', job.attempt, 'completed', payload)[0] == 'already_applied'
    stored = get(job_id)
    assert stored['result'] == payload
    assert len(events(job_id, 'completed')) == 1


def test_failure_is_a_terminal_result_too(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        job = svc.claim_next(db, 'executor-a')
        outcome, _ = svc.finish_recovery(db, job_id, 'executor-a', job.attempt, 'failed', {'error': '崩溃点超出日志范围'})
    assert outcome == 'applied'
    assert get(job_id)['status'] == 'failed'
    with Session() as db:
        with pytest.raises(HTTPException):
            svc.cancel_recovery(db, job_id, actor='operator@haili.local')


# --- 不可变性 --------------------------------------------------------------

def test_cancel_and_takeover_leave_snapshot_and_source_data_untouched(case_id):
    job_id = make_job(case_id)
    before = get(job_id)['snapshot']
    with Session() as db:
        svc.claim_next(db, 'executor-a', lease_seconds=-1)
    expire_lease(job_id)
    with Session() as db:
        new = svc.claim_next(db, 'executor-b')
        svc.finish_recovery(db, job_id, 'executor-b', new.attempt, 'completed', {'recovered': {}})
    with Session() as db:
        svc.cancel_recovery(db, make_job(case_id), actor='operator@haili.local')
        db.commit()
    stored = get(job_id)
    assert stored['snapshot'] == before
    with Session() as db:
        from src.models.entities import LogSet, CrashCase
        logs = db.scalar(select(LogSet))
        case = db.scalar(select(CrashCase))
        assert logs.records == before['log_set']['records']
        assert logs.initial == before['log_set']['initial']
        assert case.crash_after == before['case']['crash_after']


# --- worker ----------------------------------------------------------------

def test_worker_runs_queued_job_to_completed(case_id):
    job_id = make_job(case_id)
    assert claim_and_run(owner='worker-1', lock=None) is True
    stored = get(job_id)
    assert stored['status'] == 'completed'
    assert stored['result']['invariant_passed'] is True
    assert stored['owner'] == 'worker-1'
    assert claim_and_run(owner='worker-1', lock=None) is False


def test_worker_cancelled_mid_computation_does_not_publish(case_id):
    job_id = make_job(case_id)
    started, release = threading.Event(), threading.Event()

    def slow_compute(initial, records, crash_after):
        started.set()
        assert release.wait(10)
        return recover(initial, records, crash_after)

    worker = threading.Thread(target=lambda: claim_and_run(owner='worker-slow', compute=slow_compute, lock=None))
    worker.start()
    assert started.wait(10)
    with Session() as db:
        obj, changed = svc.cancel_recovery(db, job_id, actor='operator@haili.local')
        db.commit()
    assert changed is True
    release.set()
    worker.join(10)

    stored = get(job_id)
    assert stored['status'] == 'cancelled'
    assert stored['result'] is None
    assert events(job_id, 'discarded')[0]['actor'] == 'worker-slow'


def test_worker_takeover_after_executor_vanishes(case_id):
    job_id = make_job(case_id)
    with Session() as db:
        svc.claim_next(db, 'executor-gone', lease_seconds=-1)  # 领取后失联
    assert claim_and_run(owner='worker-alive', lock=None) is True
    stored = get(job_id)
    assert stored['status'] == 'completed'
    assert stored['owner'] == 'worker-alive'
    assert stored['attempt'] == 2
    assert events(job_id, 'takeover')[0]['reason'] == svc.CLAIM_REASON_TAKEOVER
    assert events(job_id, 'claimed')[0]['actor'] == 'executor-gone'


# --- HTTP API --------------------------------------------------------------

def test_api_cancel_running_job_and_detail_shows_events(client, case_id):
    headers = auth(client, 'operator')
    created = client.post('/recoveries', json={'crash_case_id': case_id, 'label': '运行中取消'}, headers=headers)
    assert created.status_code == 202
    job_id = created.json()['id']
    with Session() as db:
        svc.claim_next(db, 'worker-1', lease_seconds=60)

    res = client.post(f'/recoveries/{job_id}/cancel', json={'reason': '误触发'}, headers=headers)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body['cancel_applied'] is True
    assert body['status'] == 'cancelled'
    assert body['cancel_reason'] == '误触发'

    detail = client.get(f'/recoveries/{job_id}', headers=headers).json()
    kinds = [e['event'] for e in detail['events']]
    assert kinds == ['enqueued', 'claimed', 'cancelled']
    assert detail['events'][1]['actor'] == 'worker-1'
    assert detail['claim_reason'] == svc.CLAIM_REASON_QUEUE

    again = client.post(f'/recoveries/{job_id}/cancel', json={}, headers=headers)
    assert again.status_code == 200
    assert again.json()['cancel_applied'] is False
    assert len([e for e in again.json()['events'] if e['event'] == 'cancelled']) == 1


def test_api_viewer_cannot_cancel(client, case_id):
    operator = auth(client, 'operator')
    job_id = client.post('/recoveries', json={'crash_case_id': case_id, 'label': '观察员尝试'}, headers=operator).json()['id']
    res = client.post(f'/recoveries/{job_id}/cancel', json={}, headers=auth(client, 'viewer'))
    assert res.status_code == 403
    assert client.get(f'/recoveries/{job_id}', headers=operator).json()['status'] == 'queued'


def test_api_cancel_missing_job_returns_404(client):
    res = client.post('/recoveries/999999/cancel', json={}, headers=auth(client, 'operator'))
    assert res.status_code == 404


def test_existing_drill_runs_and_downloads_with_new_fields(client, case_id):
    headers = auth(client, 'operator')
    job_id = client.post('/recoveries', json={'crash_case_id': case_id, 'label': '既有演练'}, headers=headers).json()['id']
    assert claim_and_run(owner='worker-1', lock=None) is True

    detail = client.get(f'/recoveries/{job_id}', headers=headers)
    assert detail.status_code == 200
    body = detail.json()
    assert body['status'] == 'completed'
    assert body['result']['recovered'] == {'sample_A': 60, 'sample_B': 50}
    assert body['result']['invariant_passed'] is True
    for column in ('attempt', 'owner', 'claim_reason', 'cancel_reason', 'finished_at'):
        assert column in body

    export = client.get(f'/recoveries/{job_id}/export', headers=headers)
    assert export.status_code == 200
    assert 'attachment' in export.headers['content-disposition']
    import json
    assert json.loads(export.content)['events'][0]['event'] == 'enqueued'

    listing = client.get('/recoveries', headers=auth(client, 'viewer')).json()
    assert any(r['id'] == job_id for r in listing)


def test_export_of_cancelled_drill_has_no_result_but_keeps_snapshot(client, case_id):
    headers = auth(client, 'operator')
    created = client.post('/recoveries', json={'crash_case_id': case_id, 'label': '取消后导出'}, headers=headers).json()
    job_id = created['id']
    client.post(f'/recoveries/{job_id}/cancel', json={}, headers=headers)
    body = client.get(f'/recoveries/{job_id}/export', headers=headers).json()
    assert body['status'] == 'cancelled'
    assert body['result'] is None
    assert body['snapshot'] == created['snapshot']


def test_worker_publish_retry_after_lost_commit_does_not_duplicate(case_id):
    """首次发布提交成功但响应丢失时，重试命中 already_applied，不重复写结果或事件。"""
    job_id = make_job(case_id)
    real_finish = svc.finish_recovery

    def flaky_finish(db, ident, owner, attempt, status, result):
        outcome, obj = real_finish(db, ident, owner, attempt, status, result)
        if not getattr(flaky_finish, 'raised', False):
            flaky_finish.raised = True
            raise ConnectionError('connection lost after commit')
        return outcome, obj

    svc.finish_recovery = flaky_finish
    try:
        assert claim_and_run(owner='worker-1', compute=recover, lock=None) is True
    finally:
        svc.finish_recovery = real_finish

    stored = get(job_id)
    assert stored['status'] == 'completed'
    assert stored['result']['invariant_passed'] is True
    assert len(events(job_id, 'completed')) == 1
    assert len(events(job_id, 'discarded')) == 0
