#dispatcher_client_connection.py
#minimal implementation of dispatcher to test client connection functionality (all dummy replies)

import grpc
import datetime
import traceback
from google.protobuf.timestamp_pb2 import Timestamp
from concurrent import futures

from proto.dispatcher_pb2 import (
    RegisterResponse, LoginResponse, TaskStartResponse, TaskResult, ListCategoriesResponse, ListLocationsResponse
)

from proto.dispatcher_pb2_grpc import (
    ClientDispatcherServicer, add_ClientDispatcherServicer_to_server
)

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
        print(f"Starting task for user token: {request.token} \n Keywords: {request.keywords}, Categories: {request.categories}, Location: {request.location}, Start Time: {request.start_time.seconds}, End Time: {request.end_time.seconds}")
        return TaskStartResponse(success=True, message="Task started", task_id = f"{request.token} {request.start_time}" )
    
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
        print("Listing available categories")
        return ListCategoriesResponse(categories = ["Category 1", "Category 2", "Category 3"])

    @grpc_safe
    def ListAvailableLocations(self, request, context):
        print("Listing available locations")
        return ListLocationsResponse(locations = ["Location 1", "Location 2", "Location 3"])

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ClientDispatcherServicer_to_server(DispatcherService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Dispatcher server started on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()