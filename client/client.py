#!/usr/bin/env python3
"""
WIDE EYE Client
"""

import sys
import io
import os
import threading
import webbrowser
import time
import json
import logging
from datetime import datetime as _dt, timezone

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QDateTimeEdit, QListWidget, QListWidgetItem,
    QSplitter, QStackedWidget, QMessageBox, QDialog
)
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtCore import Qt, Signal, QTimer

import folium
import spacy
import grpc

from google.protobuf.timestamp_pb2 import Timestamp
from proto.dispatcher_pb2_grpc import ClientDispatcherStub
from proto.dispatcher_pb2 import (
    RegisterRequest, LoginRequest,
    TaskRequest, TaskResultsRequest,
    ListCategoriesRequest, ListLocationsRequest
)
from client.multi_select_search_box import MultiSelectSearchBox
from client.config import CLIENT_CONFIG

# --- Logging Setup ---
logger = logging.getLogger("WideEyeClient")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(CLIENT_CONFIG["log_file"])
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)


class MainWindow(QMainWindow):
    """
    Main application window: handles auth, task submission,
    streaming, and map-based result display.
    """
    result_received = Signal(object)
    marker_signal = Signal(dict)

    def __init__(self):
        """
        Initialize UI, load models, connect to dispatcher, and start refresh loops.
        """
        super().__init__()
        self.setWindowTitle("WIDE EYE")

        # State
        self.auth_token = ""
        self.active_tasks = {}     # task_id -> metadata
        self.all_results = {}      # task_id -> [payloads]
        self.current_task_filter = None
        self.current_task_id = None

        # Load NLP model
        logger.info("Loading spaCy model...")
        self.nlp = spacy.load("en_core_web_sm")
        logger.info(f"spaCy model loaded: {self.nlp.meta.get('name','unknown')}")

        # Load coordinates lookup
        logger.info("Loading location lookup...")
        self.location_lookup = self._load_location_lookup()
        logger.info(f"Loaded {len(self.location_lookup)} locations")

        # Build UI
        self.stack = QStackedWidget()
        self._build_auth_page()
        self._build_dashboard_page()
        self.setCentralWidget(self.stack)

        # gRPC stub
        addr = f"{CLIENT_CONFIG['dispatcher_address']}:{CLIENT_CONFIG['dispatcher_port']}"
        logger.info(f"Connecting to dispatcher at {addr}")
        channel = grpc.insecure_channel(addr)
        self.dispatcher = ClientDispatcherStub(channel)

        # Initialize map
        self._initialize_map()

        # Signals
        self.result_received.connect(self.display_single_result)
        self.marker_signal.connect(self._add_marker_to_map)

        # Load filters & start periodic refresh
        self.refresh_categories_and_locations()
        self.start_periodic_refresh()

    def _load_location_lookup(self):
        """
        Load country/state/city coordinates from JSON for NER geotagging.
        """
        path = os.path.join(os.path.dirname(__file__), "countries+states+cities.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load location JSON: {e}")
            return {}
        lookup = {}
        for country in data:
            for name, lat, lon in [
                (country["name"], country.get("latitude"), country.get("longitude"))
            ] + [
                (s["name"], s.get("latitude"), s.get("longitude"))
                for s in country.get("states", [])
            ] + [
                (c["name"], c.get("latitude"), c.get("longitude"))
                for s in country.get("states", []) for c in s.get("cities", [])
            ]:
                if lat and lon:
                    try:
                        lookup[name.lower()] = (float(lat), float(lon))
                    except:
                        pass
        return lookup

    def _build_auth_page(self):
        """
        Create the Register/Login UI.
        """
        auth = QWidget()
        layout = QVBoxLayout()
        self.user_in = QLineEdit()
        self.user_in.setPlaceholderText("Username")
        self.pass_in = QLineEdit()
        self.pass_in.setPlaceholderText("Password")
        self.pass_in.setEchoMode(QLineEdit.Password)
        reg_btn = QPushButton("Register")
        reg_btn.clicked.connect(self.on_register)
        login_btn = QPushButton("Login")
        login_btn.clicked.connect(self.on_login)
        for w in (self.user_in, self.pass_in, reg_btn, login_btn):
            layout.addWidget(w)
        auth.setLayout(layout)
        self.stack.addWidget(auth)

    def _build_dashboard_page(self):
        """
        Create the main dashboard: filters, map, and results list.
        """
        # Top filter bar
        top = QWidget(); top_layout = QHBoxLayout()
        self.keywords_input = QLineEdit(); self.keywords_input.setPlaceholderText("Keywords")
        self.categories_input = MultiSelectSearchBox(placeholder="Categories")
        self.location_input = MultiSelectSearchBox(placeholder="Locations")
        self.stime_input = QDateTimeEdit(); self.stime_input.setCalendarPopup(True)
        self.stime_input.setDateTimeRange(_dt.now().replace(year=_dt.now().year-1), _dt.now())
        self.etime_input = QDateTimeEdit(); self.etime_input.setCalendarPopup(True)
        self.etime_input.setDateTimeRange(_dt.now(), _dt.now().replace(year=_dt.now().year+1))
        add_btn = QPushButton("Add Task"); add_btn.clicked.connect(self.on_add_task)
        tasks_btn = QPushButton("Active Tasks"); tasks_btn.clicked.connect(self.on_show_active_tasks)
        for w in (
            self.keywords_input, self.categories_input, self.location_input,
            self.stime_input, self.etime_input, add_btn, tasks_btn
        ):
            top_layout.addWidget(w)
        top.setLayout(top_layout)

        # Splitter with map + results
        splitter = QSplitter(Qt.Horizontal)
        self.map_view = QWebEngineView(); splitter.addWidget(self.map_view)

        results_container = QWidget(); rc_layout = QVBoxLayout()
        self.filter_input = QLineEdit(); self.filter_input.setPlaceholderText("Filter results…")
        self.filter_input.textChanged.connect(self.on_filter_text_changed)
        self.results_list = QListWidget()
        placeholder = QListWidgetItem("Results will appear here…")
        placeholder.setFlags(placeholder.flags() & ~Qt.ItemIsSelectable)
        self.results_list.addItem(placeholder)
        self.results_list.itemDoubleClicked.connect(self.on_result_double_click)
        rc_layout.addWidget(self.filter_input); rc_layout.addWidget(self.results_list)
        results_container.setLayout(rc_layout)
        splitter.addWidget(results_container)
        splitter.setStretchFactor(0, 1); splitter.setStretchFactor(1, 2)

        dash = QWidget(); dash_layout = QVBoxLayout()
        dash_layout.addWidget(top, stretch=1)
        dash_layout.addWidget(splitter, stretch=9)
        dash.setLayout(dash_layout)
        self.stack.addWidget(dash)

    def start_periodic_refresh(self):
        """
        In a daemon thread, refresh categories/locations every 5 minutes.
        """
        def loop():
            while True:
                self.refresh_categories_and_locations()
                time.sleep(300)
        threading.Thread(target=loop, daemon=True).start()

    def refresh_categories_and_locations(self):
        """
        RPC calls to update the dropdowns with the latest catalog.
        """
        try:
            cat_resp = self.dispatcher.ListAvailableCategories(ListCategoriesRequest())
            loc_resp = self.dispatcher.ListAvailableLocations(ListLocationsRequest())
            self.categories = list(cat_resp.categories)
            self.locations = list(loc_resp.locations)
            self.categories_input.set_items(self.categories)
            self.location_input.set_items(self.locations)
            logger.info("Refreshed categories & locations")
        except Exception as e:
            logger.error(f"Filter refresh failed: {e}")

    def on_register(self):
        """
        Invoke RegisterRequest; on success, switch to dashboard.
        """
        req = RegisterRequest(
            username=self.user_in.text(),
            password=self.pass_in.text()
        )
        try:
            resp = self.dispatcher.Register(req)
        except grpc.RpcError as e:
            logger.error(f"Register RPC error: {e.details()}")
            QMessageBox.critical(self, "Network Error", e.details())
            return
        if resp.success:
            logger.info("User registered")
            QMessageBox.information(self, "Registered", resp.message)
            self.stack.setCurrentIndex(1)
        else:
            logger.warning("Registration failed")
            QMessageBox.warning(self, "Registration Failed", resp.message)

    def on_login(self):
        """
        Invoke LoginRequest; on success, store token and switch UI.
        """
        req = LoginRequest(
            username=self.user_in.text(),
            password=self.pass_in.text()
        )
        try:
            resp = self.dispatcher.Login(req)
        except grpc.RpcError as e:
            logger.error(f"Login RPC error: {e.details()}")
            QMessageBox.critical(self, "Network Error", e.details())
            return
        if resp.success:
            self.auth_token = resp.token
            logger.info(f"User logged in, token={resp.token}")
            self.stack.setCurrentIndex(1)
        else:
            logger.warning("Login failed")
            QMessageBox.warning(self, "Login Failed", resp.message)

    def _initialize_map(self):
        """
        Create an initial Folium map and load it.
        """
        logger.info("Initializing map...")
        self.folium_map = folium.Map(location=[20, 0], zoom_start=2)
        self._refresh_map_view()

    def _refresh_map_view(self):
        """
        Render the Folium map into the QWebEngineView.
        """
        buf = io.BytesIO()
        self.folium_map.save(buf, close_file=False)
        self.map_view.setHtml(buf.getvalue().decode())

    def _add_marker_to_map(self, info):
        """
        Add one marker dict {'lat','lon','tooltip','popup_text'} to the map.
        """
        folium.Marker(
            [info["lat"], info["lon"]],
            tooltip=info["tooltip"],
            popup=folium.Popup(info["popup_text"], max_width=300),
            icon=folium.Icon(icon="info-sign")
        ).add_to(self.folium_map)
        self._schedule_map_refresh()

    def _schedule_map_refresh(self):
        """
        Batch frequent marker additions into one map refresh.
        """
        if not hasattr(self, "map_refresh_pending") or not self.map_refresh_pending:
            self.map_refresh_pending = True
            QTimer.singleShot(2000, self._perform_map_refresh)

    def _perform_map_refresh(self):
        """
        Actually updates the HTML in the map view.
        """
        self._refresh_map_view()
        self.map_refresh_pending = False

    def to_ts(self, qdt):
        """
        Convert a QDateTimeEdit to a UTC Timestamp proto.
        """
        ts = Timestamp()
        dt_local = qdt.dateTime().toPython()
        local_tz = _dt.now().astimezone().tzinfo
        dt_with_tz = dt_local.replace(tzinfo=local_tz)
        ts.FromDatetime(dt_with_tz.astimezone(timezone.utc))
        return ts

    def on_add_task(self):
        """
        Build TaskRequest, call StartTask, then begin streaming results.
        """
        logger.info("Submitting TaskRequest")
        req = TaskRequest(
            token=self.auth_token,
            keywords=self.keywords_input.text(),
            categories=",".join(self.categories_input.selected_items()),
            location=",".join(self.location_input.selected_items()),
            start_time=self.to_ts(self.stime_input),
            end_time=self.to_ts(self.etime_input),
        )
        resp = self.dispatcher.StartTask(req)
        if resp.success:
            self.current_task_id = resp.task_id
            self.active_tasks[resp.task_id] = {
                "keywords": req.keywords,
                "categories": self.categories_input.selected_items(),
                "locations": self.location_input.selected_items(),
            }
            QMessageBox.information(self, "Task Added", resp.message)
            self._stream_results_loop()
        else:
            QMessageBox.warning(self, "Add Task Failed", resp.message)

    def _stream_results_loop(self):
        """
        Daemon thread that consumes StreamResults and emits signals.
        """
        def loop():
            for res in self.dispatcher.StreamResults(
                TaskResultsRequest(token=self.auth_token, task_id=self.current_task_id)
            ):
                self.result_received.emit(res)
        threading.Thread(target=loop, daemon=True).start()

    def display_single_result(self, result):
        """
        Handle one TaskResult: JSON decode, NER→markers, and add to list.
        """
        payload = json.loads(result.result)
        tid = payload["task_id"]
        self.all_results.setdefault(tid, []).append(payload)

        # NER geotagging
        doc = self.nlp(payload.get("title", ""))
        payload["marker_coords"] = []
        for ent in doc.ents:
            if ent.label_ in ("GPE", "LOC"):
                coord = self.location_lookup.get(ent.text.strip().lower())
                if coord:
                    info = {
                        "lat": coord[0],
                        "lon": coord[1],
                        "tooltip": ent.text.strip(),
                        "popup_text": payload["title"],
                    }
                    payload["marker_coords"].append(info)
                    if self.current_task_filter in (None, tid):
                        self.marker_signal.emit(info)

        # Add list item
        if self.current_task_filter in (None, tid):
            text = f"{payload['title']}\n{payload.get('published','')} - {payload.get('source','')}"
            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, payload.get("link", ""))
            if self.results_list.count() == 1 and "Results will appear here" in self.results_list.item(0).text():
                self.results_list.takeItem(0)
            self.results_list.addItem(item)

    def on_filter_text_changed(self, text):
        """
        Hide/show list items based on substring match.
        """
        for i in range(self.results_list.count()):
            item = self.results_list.item(i)
            item.setHidden(text.lower() not in item.text().lower())

    def on_show_active_tasks(self):
        """
        Popup dialog for selecting which task’s results to display.
        """
        dlg = QDialog(self)
        dlg.setWindowTitle("Select Active Task")
        dlg.resize(400, 300)
        layout = QVBoxLayout(dlg)
        lw = QListWidget()
        all_item = QListWidgetItem("[All Tasks]")
        all_item.setData(Qt.UserRole, None)
        lw.addItem(all_item)
        for tid, md in self.active_tasks.items():
            text = f"[{tid[:6]}] KW:{md['keywords']} | CAT:{','.join(md['categories'])} | LOC:{','.join(md['locations'])}"
            itm = QListWidgetItem(text)
            itm.setData(Qt.UserRole, tid)
            lw.addItem(itm)
        lw.itemClicked.connect(lambda i, dlg=dlg: self._on_task_selected(i, dlg))
        layout.addWidget(lw)
        dlg.exec()

    def _on_task_selected(self, item, dlg):
        """
        Apply the chosen task filter: rebuild map & list.
        """
        self.current_task_filter = item.data(Qt.UserRole)
        # Rebuild map
        self.folium_map = folium.Map(location=[20, 0], zoom_start=2)
        for tid, lst in self.all_results.items():
            if self.current_task_filter in (None, tid):
                for p in lst:
                    for info in p.get("marker_coords", []):
                        folium.Marker(
                            [info["lat"], info["lon"]],
                            tooltip=info["tooltip"],
                            popup=folium.Popup(info["popup_text"], max_width=300),
                            icon=folium.Icon(icon="info-sign"),
                        ).add_to(self.folium_map)
        self._refresh_map_view()

        # Rebuild list
        self.results_list.clear()
        found = False
        for tid, lst in self.all_results.items():
            if self.current_task_filter in (None, tid):
                for p in lst:
                    it = QListWidgetItem(f"{p['title']}\n{p.get('published','')} - {p.get('source','')}")
                    it.setData(Qt.UserRole, p.get("link",""))
                    self.results_list.addItem(it)
                    found = True
        if not found:
            ph = QListWidgetItem("No results for this task.")
            ph.setFlags(ph.flags() & ~Qt.ItemIsSelectable)
            self.results_list.addItem(ph)
        dlg.accept()

    def on_result_double_click(self, item):
        """
        Open a result’s URL in the default browser.
        """
        url = item.data(Qt.UserRole)
        if url:
            webbrowser.open(url)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    window.stack.setCurrentIndex(0)  # start at login
    sys.exit(app.exec())
