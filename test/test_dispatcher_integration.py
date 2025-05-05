import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import datetime
import time

from google.protobuf.timestamp_pb2 import Timestamp

from dispatcher.dispatcher import DispatcherService
from dispatcher.task_manager import TaskManager
from dispatcher.collector_manager import CollectorManager
from proto.dispatcher_pb2 import TaskRequest
from dispatcher.source_catalog import load_sources

# Dummy gRPC context for grpc_safe wrapper
def DummyContext():
    class Ctx:
        def set_details(self, d): pass
        def set_code(self, c):    pass
    return Ctx()

def make_timestamp(dt: datetime.datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts

@pytest.fixture
def dispatcher(tmp_path):
    """Create a fresh DispatcherService instance with a temp database and collector."""
    db_file = tmp_path / "tasks.db"
    svc = DispatcherService(task_manager=TaskManager(db_path=str(db_file)),
                            collector_manager=CollectorManager(),
                            sources=load_sources("dispatcher/sources.json"))

    # Override the persistent task manager to use a temp DB
    svc.task_manager = TaskManager(db_path=str(db_file))

    # Override the collector manager in the service
    svc.collector_manager = CollectorManager()

    # Register, login, and heartbeat a collector
    ok, msg = svc.collector_manager.register_collector("collector1", "secret1")
    assert ok, msg
    ok, token, msg = svc.collector_manager.login_collector("collector1", "secret1")
    assert ok, msg
    ok, msg = svc.collector_manager.heartbeat(token)
    assert ok, msg
    svc.test_collector_token = token

    return svc


def test_start_task_success(dispatcher):
    """Verify that starting a valid task correctly saves it and assigns sources."""

    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    future = now + datetime.timedelta(minutes=5)

    req = TaskRequest(
        token="test-token",
        keywords="some keywords",
        categories="general",
        location="international",
        start_time=make_timestamp(now),
        end_time=make_timestamp(future)
    )

    ctx = DummyContext()
    resp = dispatcher.StartTask(req, ctx)

    assert resp.success
    assert resp.task_id

    # Check that the task is saved in the persistent store
    task = dispatcher.task_manager.get_task(resp.task_id)
    assert task is not None
    assert task["status"] == "DISPATCHED"
    assert task["keywords"] == "some keywords"

    # Check that the collector_manager has the assignment
    collector_info = dispatcher.collector_manager.get_collector_info("collector1")
    assert collector_info is not None

    assigned_tasks = collector_info.get_tasks()
    assert resp.task_id in assigned_tasks

    assigned_sources = assigned_tasks[resp.task_id]["sources"]
    assert len(assigned_sources) > 0


def test_start_task_no_matching_sources(dispatcher):
    """Verify that starting a task with no matching sources fails cleanly."""

    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    future = now + datetime.timedelta(minutes=5)

    req = TaskRequest(
        token="test-token",  
        keywords="some other keywords",
        categories="this-category-does-not-exist",
        location="this-location-does-not-exist",
        start_time=make_timestamp(now),
        end_time=make_timestamp(future)
    )

    ctx = DummyContext()
    resp = dispatcher.StartTask(req, ctx)

    assert not resp.success
    assert resp.task_id == ""
