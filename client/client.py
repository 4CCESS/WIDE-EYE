#!/usr/bin/env python3
# globe_sense_client.py
# Skeleton for GlobeSense OSINT client using PySide6 and folium map embedding
# Modified: skip login for now to directly show main window

import sys
import io
import threading

from datetime import datetime

# PySide6 imports
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QDateTimeEdit, QListWidget, QListWidgetItem,
    QSplitter, QStackedWidget, QMessageBox
)
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtCore import Qt, QUrl

# For map generation
import folium

# For gRPC
import grpc
from proto.dispatcher_pb2_grpc import ClientDispatcherStub
from proto.dispatcher_pb2 import (
    RegisterRequest, LoginRequest,
    TaskRequest, TaskResultsRequest,
)

from google.protobuf.timestamp_pb2 import Timestamp


class MainWindow(QMainWindow):
    """
    Main application window, stacked widget for auth and dashboard.
    """
    def __init__(self):
        super().__init__()

        #1) Stacked widget for login and main window
        #Stack: 0 - Auth, 1 - Main
        self.stack = QStackedWidget()

        #2) Build auth/dashboard pages
        self._build_auth_page()
        self._build_dashboard_page()

        #3) Set stacked widget as central widget
        self.setCentralWidget(self.stack)

        #4) Initialize gRPC channel and dispatcher stub
        #TODO: move to config
        #secure implementation
        #creds = grpc.ssl_channel_credentials()
        #channel = grpc.secure_channel('localhost:50051', creds)

        #insecure implementation
        channel = grpc.insecure_channel('localhost:50051')
        self.dispatcher = ClientDispatcherStub(channel)

    def _build_auth_page(self):
        auth = QWidget()
        layout = QVBoxLayout()

        # Username and password input
        self.user_in = QLineEdit()
        self.user_in.setPlaceholderText("Username")
        self.pass_in = QLineEdit()
        self.pass_in.setEchoMode(QLineEdit.Password)
        self.pass_in.setPlaceholderText("Password")

        # Buttons
        reg_btn   = QPushButton("Register")
        login_btn = QPushButton("Login")
        reg_btn.clicked.connect(self.on_register)
        login_btn.clicked.connect(self.on_login)

        # Assemble
        for w in (self.user_in, self.pass_in, reg_btn, login_btn):
            layout.addWidget(w)
        auth.setLayout(layout)

        # Add to stack as page 0
        self.stack.addWidget(auth)

    def _build_dashboard_page(self):

        # Top bar: keyword, location, timeframe, buttons
        top = QWidget()
        top_l = QHBoxLayout()
        

        self.keywords_input = QLineEdit()
        self.keywords_input.setPlaceholderText("Keywords")
        self.location_input = QLineEdit()
        self.location_input.setPlaceholderText("Relevant Locations")
        self.stime_input = QDateTimeEdit()
        self.stime_input.setCalendarPopup(True)
        self.stime_input.setDateTimeRange(
            datetime.now().replace(year=datetime.now().year - 1),
            datetime.now()
        )
        self.stime_input.setToolTip("Select start of task timeframe")
        self.etime_input = QDateTimeEdit()
        self.etime_input.setCalendarPopup(True)
        self.etime_input.setDateTimeRange(
            datetime.now(),
            datetime.now().replace(year=datetime.now().year + 1)
        )
        self.etime_input.setToolTip("Select end of task timeframe")

        self.add_task_btn = QPushButton("Add Task")
        self.add_task_btn.clicked.connect(self.on_add_task)
        self.active_tasks_btn = QPushButton("Active Tasks")
        self.active_tasks_btn.clicked.connect(self.on_show_active_tasks)

        for widget in [self.keywords_input, self.location_input,
                       self.stime_input, self.etime_input, self.add_task_btn,
                       self.active_tasks_btn]:
            top_l.addWidget(widget)

        top.setLayout(top_l)

        # Bottom: splitter with map on left, results on right
        splitter = QSplitter(Qt.Horizontal)

        # Left: scrollable map
        self.map_view = QWebEngineView()
        self._load_map()

        # TODO: Connect map click JS events to Python slot for reverse geocoding

        # Right: results list
        self.results_list = QListWidget()
        placeholder = QListWidgetItem("Results will appear here...")
        placeholder.setFlags(placeholder.flags() & ~Qt.ItemIsSelectable)
        self.results_list.addItem(placeholder)

        splitter.addWidget(self.map_view)
        splitter.addWidget(self.results_list)
        splitter.setStretchFactor(0, 1) # map
        splitter.setStretchFactor(1, 1) # results

        # Combine 
        dash = QWidget()
        dash_layout = QVBoxLayout()
        dash_layout.addWidget(top, stretch=0.5)
        dash_layout.addWidget(splitter, stretch=6)
        dash.setLayout(dash_layout)

        # Add to stack as page 1
        self.stack.addWidget(dash)

    def on_register(self):
        req = RegisterRequest(
            username=self.user_in.text(),
            password=self.pass_in.text()
        )
        try:
            resp = self.dispatcher.Register(req)
            if resp.success:
                # Show success popup
                QMessageBox.information(self, "Registration Successful", resp.message)
                # Automatically switch to dashboard
                self.stack.setCurrentIndex(1)
            else:
                # Show error popup
                QMessageBox.warning(self, "Registration Failed", resp.message)
        except grpc.RpcError as e:
            # If gRPC itself fails (e.g., server down), show an error
            QMessageBox.critical(self, "Network Error", e.details())

    def on_login(self):
        req = LoginRequest(
            username=self.user_in.text(),
            password=self.pass_in.text()
        )
        resp = self.dispatcher.Login(req)
        if resp.success:
            self.auth_token = resp.token
            self.stack.setCurrentIndex(1)
        else:
            # show resp.message
            pass


    def _load_map(self):
        """
        Generate initial folium map and load into QWebEngineView.
        """
        m = folium.Map(location=[20, 0], zoom_start=2)
        data = io.BytesIO()
        m.save(data, close_file=False)
        html = data.getvalue().decode()
        self.map_view.setHtml(html)

    def to_ts(self, qdt):
        ts = Timestamp()
        dt = qdt.toPython()
        ts.FromDatetime(dt)
        return ts

    def on_add_task(self):
        """
        Triggered when user clicks 'Add Task'.
        Build task payload and send to dispatcher.
        """

        token = self.auth_token
        keywords = self.keywords_input.text()
        location = self.location_input.text()
        start_time = self.to_ts(self.stime_input)
        end_time = self.to_ts(self.etime_input)

        req = TaskRequest(
            token = token,
            keywords = keywords,
            location = location,
            start_time = start_time,
            end_time = end_time
        )
        resp = self.dispatcher.StartTask(req)
        if resp.success:
            self.current_task_id = resp.task_id
            self._start_result_stream()
            QMessageBox.information(self, "Task Added", resp.message)
        else:
            QMessageBox.warning(self, "Task Addition Failed", resp.message)

        # TODO: Construct and send gRPC TaskRequest
        print(f"Sending task: {keywords}, {location}, from {start_time} to {end_time}")

    def on_show_active_tasks(self):
        """
        Show a dialog or sidebar with active tasks pulled from dispatcher.
        """
        print("Active tasks dialog placeholder")



    def _start_result_stream(self):
        def run_stream():
            req = TaskResultsRequest(
                token=self.auth_token,
                task_id=self.current_task_id
            )
            for result in self.dispatcher.StreamResults(req):
                QApplication.instance().invokeMethod(
                    self.display_single_result,
                    Qt.QueuedConnection,
                    result
                )
        threading.Thread(target=run_stream, daemon=True).start()

    def display_single_result(self, result):
        item = QListWidgetItem(result.title)
        item.setData(Qt.UserRole, result.url)
        self.results_list.addItem(item)



def main():
    app = QApplication(sys.argv)

    # Directly launch the main window without login for development
    mw = MainWindow()
    mw.show()
    mw.stack.setCurrentIndex(1)

    sys.exit(app.exec())


if __name__ == '__main__':
    main()
