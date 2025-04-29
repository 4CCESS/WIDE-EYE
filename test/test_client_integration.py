import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime
import pytest

from google.protobuf.timestamp_pb2 import Timestamp
from PySide6.QtCore import QDateTime
from PySide6.QtWidgets import QApplication

import client.client as client_mod
from client.multi_select_search_box import MultiSelectSearchBox
from proto.dispatcher_pb2 import TaskRequest

# Fake dispatcher stub to intercept calls
class FakeDispatcher:
    def __init__(self):
        self.last_req = None
        class StartResp:
            success = True
            message = "OK"
            task_id = "fake-task-id"
        self._start_resp = StartResp()
    def ListAvailableCategories(self, req):
        return type("Resp", (), {"categories": ["catA", "catB"]})()
    def ListAvailableLocations(self, req):
        return type("Resp", (), {"locations": ["locX"]})()
    def StartTask(self, req: TaskRequest):
        self.last_req = req
        return self._start_resp
    def StreamResults(self, req):
        return iter([])  # no-op

@pytest.fixture(scope="module")
def qt_app():
    app = QApplication.instance() or QApplication([])
    yield app

@pytest.fixture
def main_window(qt_app, monkeypatch):
    # Patch the ClientDispatcherStub before constructing MainWindow
    fake = FakeDispatcher()
    monkeypatch.setattr(
        client_mod, 'ClientDispatcherStub',
        lambda channel: fake
    )
    mw = client_mod.MainWindow()
    # Ensure the stub in instance is our fake
    mw.dispatcher = fake
    return mw


def make_ts(dt: datetime.datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def test_on_add_task_constructs_request(main_window):
    mw = main_window
    # configure inputs
    mw.auth_token = "tok123"
    mw.keywords_input.setText("kw1")
    # Provide selected_items on multi-select widgets
    mw.categories_input.selected_items = lambda: ["catA", "catB"]
    mw.location_input.selected_items   = lambda: ["locX"]
    # set specific datetimes
    dt1 = datetime.datetime(2025, 4, 28, 12, 0)
    dt2 = datetime.datetime(2025, 4, 29, 12, 30)
    mw.stime_input.setDateTime(QDateTime(dt1))
    mw.etime_input.setDateTime(QDateTime(dt2))

    # invoke the action
    mw.on_add_task()

    # inspect captured TaskRequest
    req = mw.dispatcher.last_req
    assert isinstance(req, TaskRequest)
    assert req.token == "tok123"
    assert req.keywords == "kw1"
    assert req.categories == "catA,catB"
    assert req.location   == "locX"

    # verify timestamps round-trip correctly
    st = req.start_time.ToDatetime()
    et = req.end_time.ToDatetime()
    assert st.year == 2025 and st.month == 4 and st.day == 28 and st.hour == 12 and st.minute == 0
    assert et.year == 2025 and et.month == 4 and et.day == 29 and et.hour == 12 and et.minute == 30
