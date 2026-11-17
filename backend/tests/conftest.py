import os
import tempfile

_DB = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
_DB.close()
os.environ.setdefault('DATABASE_URL', 'sqlite:///' + _DB.name)
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6399/0')

import pytest
from src.config.database import Base, Session, engine
from src.config.schema import ensure_schema
from src.models.entities import User, LogSet, CrashCase
from src.utils.security import hash_password

LOG_RECORDS = [
    {'op': 'begin', 'tx': 'T1'}, {'op': 'write', 'tx': 'T1', 'key': 'sample_A', 'value': 75},
    {'op': 'commit', 'tx': 'T1'}, {'op': 'checkpoint'},
    {'op': 'begin', 'tx': 'T2'}, {'op': 'write', 'tx': 'T2', 'key': 'sample_B', 'value': 40},
    {'op': 'begin', 'tx': 'T3'}, {'op': 'write', 'tx': 'T3', 'key': 'sample_A', 'value': 60},
    {'op': 'commit', 'tx': 'T3'}, {'op': 'write', 'tx': 'T2', 'key': 'sample_B', 'value': 35},
    {'op': 'commit', 'tx': 'T2'},
]


@pytest.fixture(autouse=True)
def clean_db():
    Base.metadata.drop_all(engine)
    ensure_schema()
    with Session() as db:
        for role in ('admin', 'operator', 'viewer'):
            db.add(User(email=role + '@haili.local', role=role, password_hash=hash_password('Recovery2026!')))
        logs = LogSet(name='晚班', initial={'sample_A': 100, 'sample_B': 50}, records=LOG_RECORDS, description='测试')
        db.add(logs); db.flush()
        db.add(CrashCase(name='断电点10', log_set_id=logs.id, crash_after=10))
        db.commit()
    yield


@pytest.fixture
def db():
    with Session() as session:
        yield session


@pytest.fixture
def case_id():
    with Session() as session:
        from sqlalchemy import select
        return session.scalar(select(CrashCase.id))


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from src.main import app
    return TestClient(app)


def auth(client, role='operator'):
    res = client.post('/auth/login', json={'email': role + '@haili.local', 'password': 'Recovery2026!'})
    assert res.status_code == 200, res.text
    return {'Authorization': 'Bearer ' + res.json()['access_token']}
