#dispatcher.py


import grpc
import datetime
import traceback
import json
from google.protobuf.timestamp_pb2 import Timestamp
from concurrent import futures

from proto.dispatcher_pb2 import (
    RegisterResponse, LoginResponse, TaskStartResponse, TaskResult, ListCategoriesResponse, ListLocationsResponse
)

from proto.dispatcher_pb2_grpc import (
    ClientDispatcherServicer, add_ClientDispatcherServicer_to_server
)

from dispatcher.source_catalog import load_sources, list_available_categories, list_available_locations, match_sources
from dispatcher.task_manager import TaskManager

def grpc_safe(f):
    def wrapper(self, request, context):
        try:
            return f(self, request, context)
        except Exception as e:
            print(f"[ERROR] Exception in {f.__name__}: {e}")
            traceback.print_exc()
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return None
    return wrapper

class DispatcherService(ClientDispatcherServicer):
    @grpc_safe
    def Register(self, request, context):
        print(f"Registering client: {request.username}")
        return RegisterResponse(success=True, message="Registration successful", user_id = request.username)
    
    @grpc_safe
    def Login(self, request, context):
        print(f"Logging in client: {request.username}")
        return LoginResponse(success=True, message="Login successful", token = "1234567890")
    
    @grpc_safe
    def StartTask(self, request, context):
        # 1. Parse the comma-separated strings into Python lists
        task_cats = [c.strip() for c in request.categories.split(',') if c.strip()]
        task_locs = [l.strip() for l in request.location.split(',') if l.strip()]

        # 2. Load your full catalog and match
        self.sources = load_sources("dispatcher/sources.json")
        matched = match_sources(task_cats, task_locs, self.sources)

        # 3. (For now) Print matched source IDs so you can see it
        print(f"Matched sources for categories={task_cats} locations={task_locs}:",
              [s["id"] for s in matched])

        # 4. Then proceed as before — e.g. schedule collectors for `matched`
        return TaskStartResponse(
            success=True,
            message=f"Task started, matched {len(matched)} sources",
            task_id=f"{request.token} {request.start_time}"
        )

    
    @grpc_safe
    def StreamResults(self, request, context):
        print(f"Result stream for task: {request.task_id}")
        ts = Timestamp()
        ts.FromDatetime(datetime.datetime.now(datetime.timezone.utc))  # Use current time
        yield TaskResult(task_id = request.task_id, title = "Task 1", url = "https://www.google.com", timestamp = ts)
        yield TaskResult(task_id = request.task_id, title = "Task 2", url = "https://www.google.com", timestamp = ts)
        yield TaskResult(task_id = request.task_id, title = "Task 3", url = "https://www.google.com", timestamp = ts)
        yield TaskResult(task_id = request.task_id, title = "Task 4", url = "https://www.google.com", timestamp = ts)
        yield TaskResult(task_id = request.task_id, title = "Task 5", url = "https://www.google.com", timestamp = ts)


    @grpc_safe
    def ListAvailableCategories(self, request, context):
        self.sources = load_sources("dispatcher/sources.json")
        print("Listing available categories")
        return ListCategoriesResponse(categories = list_available_categories(self.sources))

    @grpc_safe
    def ListAvailableLocations(self, request, context):
        print("Listing available locations")
        return ListLocationsResponse(locations = list_available_locations(self.sources))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ClientDispatcherServicer_to_server(DispatcherService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Dispatcher server started on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()