import logging, os, socket, threading, time
from pathlib import Path
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.services.recovery_service import LEASE_SECONDS, claim_next, finish_recovery, renew_lease
from src.services.wal_engine import recover
redis = Redis.from_url(REDIS_URL)
logging.basicConfig(level=logging.INFO)
WORKER_ID = f'{socket.gethostname()}:{os.getpid()}'
HEARTBEAT_SECONDS = max(1, LEASE_SECONDS // 3)


def _heartbeat(stop, lock, ident, owner, attempt, lease_seconds):
    """Keep the lease alive while the pure-Python computation runs."""
    interval = max(1, lease_seconds // 3) if lease_seconds > 0 else 1
    while not stop.wait(interval):
        Path('/tmp/heartbeat').touch()
        try:
            with Session() as db:
                alive = renew_lease(db, ident, owner, attempt, lease_seconds)
            if not alive:
                logging.warning('recovery %s lease lost (attempt %s)', ident, attempt)
                return
            if lock is not None:
                # Losing the Redis leadership lock is not fatal: claims are
                # conditional UPDATEs, so two executors still cannot own a job.
                try: lock.extend(15)
                except Exception: logging.warning('leadership lock lost or expired')
        except Exception:
            logging.exception('heartbeat failed for recovery %s', ident)


def claim_and_run(owner=WORKER_ID, compute=recover, lease_seconds=LEASE_SECONDS, lock=None, db_factory=Session):
    """Claim one job and execute it under a lease.

    Returns True if a job was picked up. A lost lease or a takeover never blocks
    finish_recovery(): the attempt number fences the late writer out.
    """
    with db_factory() as db:
        job = claim_next(db, owner, lease_seconds)
        if job is None: return False
        ident, attempt, snapshot = job.id, job.attempt, job.snapshot
    logging.info('recovery %s claimed by %s (attempt %s, %s)', ident, owner, attempt, job.claim_reason)

    stop = threading.Event()
    beat = threading.Thread(target=_heartbeat, args=(stop, lock, ident, owner, attempt, lease_seconds), daemon=True)
    beat.start()
    try:
        try:
            result = compute(snapshot['log_set']['initial'], snapshot['log_set']['records'], snapshot['case']['crash_after'])
            status = 'completed'
        except ValueError as exc:
            result = {'error': str(exc), 'simulation': True}
            status = 'failed'
        except Exception as exc:
            logging.exception('recovery %s computation failed', ident)
            result = {'error': f'计算异常：{exc}', 'simulation': True}
            status = 'failed'
    finally:
        stop.set(); beat.join(timeout=HEARTBEAT_SECONDS + 1)
    # A publish retry is safe: finish_recovery either applies once or reports
    # already_applied when the first attempt committed but the reply was lost.
    outcome, obj = 'missing', None
    for attempt_no in range(3):
        try:
            with db_factory() as db:
                outcome, obj = finish_recovery(db, ident, owner, attempt, status, result)
            break
        except Exception:
            logging.exception('recovery %s publish retry %s', ident, attempt_no + 1)
            time.sleep(1)
    if outcome == 'applied':
        logging.info('recovery %s %s', ident, status)
    elif outcome == 'already_applied':
        logging.info('recovery %s %s already applied, retry skipped', ident, status)
    else:
        current = obj.status if obj is not None else 'missing'
        logging.warning('recovery %s result discarded (attempt %s, current %s)', ident, attempt, current)
    return True


def main():
    # Redis leadership lock keeps a single active worker; the DB lease is the
    # source of truth for ownership and lets a new worker take over dead ones.
    while True:
        Path('/tmp/heartbeat').touch()
        # thread_local=False lets the lease heartbeat thread extend the same lock.
        lock = redis.lock('wal-worker', timeout=15, blocking_timeout=1, thread_local=False)
        try:
            with lock:
                while claim_and_run(lock=lock): pass
        except Exception:
            logging.exception('worker retry')
        time.sleep(1)


if __name__ == '__main__':
    main()
