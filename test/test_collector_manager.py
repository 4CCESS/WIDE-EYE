import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from dispatcher.collector_manager import CollectorManager

@pytest.fixture
def cm():
    return CollectorManager()

def test_register_and_login(cm):
    ok, msg = cm.register_collector("col1", "secret")
    assert ok
    ok, token, _ = cm.login_collector("col1", "secret")
    assert ok and token

def test_heartbeat_and_metrics(cm):
    cm.register_collector("col1", "secret")
    ok, token, _ = cm.login_collector("col1", "secret")
    ok, _ = cm.heartbeat(token)
    assert ok

    metrics = cm.get_collector_metrics("col1")
    assert metrics["heartbeat_count"] >= 1

def test_task_assignment_and_expiry(cm):
    cm.register_collector("colA", "secA")
    ok, token, _ = cm.login_collector("colA", "secA")

    # assign one task, short expiry
    end = time.time() + 0.1
    ok, _ = cm.assign_task_balanced("taskX", ["src1"], end)
    assert ok

    # wait for expiry
    time.sleep(0.2)

    # check BEFORE purging
    assert cm.has_task_expired("taskX")

    # purge now
    expired = cm.purge_expired_tasks()
    assert any(tid == "taskX" for _, tid in expired)

    # after purge, task no longer tracked
    assert not cm.has_task_expired("taskX")

