import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import pytest

from google.protobuf.timestamp_pb2 import Timestamp

from dispatcher.dispatcher import CollectorDispatcherService
from dispatcher.task_manager import TaskManager
from dispatcher.collector_manager import CollectorManager
from proto.dispatcher_pb2 import TaskStreamRequest, TaskAssignment
from dispatcher.source_catalog import load_sources

class DummyContext:
    def abort(self, code, details):
        raise RuntimeError(f"Abort called with {code}: {details}")

@pytest.fixture
def service_and_managers(tmp_path):
    # Create shared managers with an isolated SQLite file
    db_file = tmp_path / "tasks.db"
    task_manager = TaskManager(db_path=str(db_file))
    collector_manager = CollectorManager()
    sources = load_sources("dispatcher/sources.json")

    # Instantiate service and override its managers
    svc = CollectorDispatcherService(task_manager, collector_manager, [])

    return svc, task_manager, collector_manager

def test_stream_tasks_yields_assignment(service_and_managers):
    svc, task_mgr, coll_mgr = service_and_managers

    # Register and login a collector
    ok, msg = coll_mgr.register_collector("colX", "secX")
    assert ok, msg
    ok, token, msg = coll_mgr.login_collector("colX", "secX")
    assert ok, msg
    ok, msg = coll_mgr.heartbeat(token)
    assert ok, msg

    # Prepare a task in the task manager
    now = datetime.datetime.now(datetime.timezone.utc)
    later = now + datetime.timedelta(minutes=10)
    start_iso = now.isoformat()
    end_iso = later.isoformat()

    task_id = "task123"
    task_mgr.create_task(
        task_id=task_id,
        token="clientTok",
        keywords="search-term",
        categories=["cat1"],
        locations=["loc1"],
        start_time=start_iso,
        end_time=end_iso
    )

    # Assign two sources to the collector
    end_ts = later.timestamp()
    ok, msg = coll_mgr.assign_task_to_collector(token, task_id, ["srcA", "srcB"], end_ts)
    assert ok, msg

    # Stream assignments
    req = TaskStreamRequest(token=token)
    gen = svc.StreamTasks(req, DummyContext())

    # Grab the first assignment
    ta = next(gen)
    assert isinstance(ta, TaskAssignment)
    assert ta.task_id == task_id
    assert ta.keywords == "search-term"
    assert ta.category == "cat1"
    assert ta.location == "loc1"
    assert ta.start_time.ToDatetime().isoformat() == start_iso
    assert ta.end_time.ToDatetime().isoformat() == end_iso

    # Sources list matches
    assert list(ta.sources) == ["srcA", "srcB"]
