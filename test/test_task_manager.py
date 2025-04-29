import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json
import datetime
from datetime import timedelta
import tempfile

import pytest
from dispatcher.task_manager import TaskManager

@pytest.fixture
def temp_db(tmp_path):
    db = tmp_path / "tasks.db"
    return str(db)

def test_task_lifecycle(temp_db):
    tm = TaskManager(db_path=temp_db)
    assert tm.count_tasks() == 0

    # Create a task
    start = datetime.datetime.now(datetime.timezone.utc).isoformat()
    end   = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).isoformat()
    tm.create_task(
        task_id="t1",
        token="tok1",
        keywords="kw1",
        categories=["cat1"],
        locations=["loc1"],
        start_time=start,
        end_time=end
    )
    assert tm.count_tasks() == 1

    # Fetch and verify
    t = tm.get_task("t1")
    assert t["task_id"] == "t1"
    assert t["token"] == "tok1"
    assert t["keywords"] == "kw1"
    assert t["status"] == "PENDING"

    # Status transitions
    tm.mark_dispatched("t1")
    assert tm.get_task("t1")["status"] == "DISPATCHED"
    tm.mark_completed("t1")
    assert tm.get_task("t1")["status"] == "COMPLETED"
    tm.mark_failed("t1")
    assert tm.get_task("t1")["status"] == "FAILED"
    tm.cancel_task("t1")
    assert tm.get_task("t1")["status"] == "CANCELLED"

    # Listing & filtering
    tm.create_task("t2", "tok1", "kw2", ["cat1"], ["loc1"], start, end)
    all_tasks = tm.list_tasks()
    assert len(all_tasks) == 2

    pendings = tm.list_tasks_by_status(["PENDING"])
    assert all(t["status"] == "PENDING" for t in pendings)

    by_token = tm.list_tasks(token="tok1")
    assert len(by_token) == 2
