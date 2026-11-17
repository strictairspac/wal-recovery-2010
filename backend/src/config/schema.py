"""Idempotent schema top-ups for the durable recovery queue.

The project has no online migration runner; create_all only builds missing
tables. Existing deployments need the queue columns added in place, otherwise
old drills could not be listed or cancelled after upgrade.
"""
from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src.models import entities  # noqa: F401  (registers tables on Base.metadata)

_COLUMNS = (
    ('recoveries', 'attempt', 'INTEGER NOT NULL DEFAULT 0'),
    ('recoveries', 'owner', 'VARCHAR(120)'),
    ('recoveries', 'claimed_at', 'TIMESTAMP'),
    ('recoveries', 'heartbeat_at', 'TIMESTAMP'),
    ('recoveries', 'lease_expires_at', 'TIMESTAMP'),
    ('recoveries', 'claim_reason', 'VARCHAR(300)'),
    ('recoveries', 'cancel_reason', 'VARCHAR(300)'),
    ('recoveries', 'finished_at', 'TIMESTAMP'),
)

def ensure_schema():
    Base.metadata.create_all(engine)
    insp = inspect(engine)
    for table, column, ddl in _COLUMNS:
        existing = {c['name'] for c in insp.get_columns(table)}
        if column not in existing:
            with engine.begin() as conn:
                conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}'))
    with engine.begin() as conn:
        conn.execute(text('CREATE INDEX IF NOT EXISTS ix_recovery_events_recovery_id ON recovery_events (recovery_id)'))
