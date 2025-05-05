#!/usr/bin/env python3
import sys
import os
import grpc
from datetime import datetime, timezone, timedelta

# Ensure Python can find your generated proto modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto.dispatcher_pb2_grpc import ClientDispatcherStub
from proto.dispatcher_pb2 import LoginRequest, TaskRequest, TaskResultsRequest
from google.protobuf.timestamp_pb2 import Timestamp

# 1) Connect & login
channel = grpc.insecure_channel('localhost:50051')
stub    = ClientDispatcherStub(channel)
login   = stub.Login(LoginRequest(username='demo', password='demo'))
token   = login.token

# 2) Build a 2-minute task window starting now (UTC‐aware)
now      = datetime.now(timezone.utc)
start_ts = Timestamp()
start_ts.FromDatetime(now)
end_ts   = Timestamp()
end_ts.FromDatetime(now + timedelta(minutes=2))

# 3) Start the task (must match your sources.json categories/locations)
task_rsp = stub.StartTask(TaskRequest(
    token      = token,
    keywords   = "",
    categories = "general",
    location   = "international",
    start_time = start_ts,
    end_time   = end_ts
))
print("Task started:", task_rsp.task_id)

# 4) Stream results until the dispatcher closes the stream
for result in stub.StreamResults(TaskResultsRequest(token=token, task_id=task_rsp.task_id)):
    print("→", result.result)
