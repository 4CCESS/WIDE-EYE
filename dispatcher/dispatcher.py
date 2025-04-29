#dispatcher.py


import grpc
import datetime
import traceback
import json
import uuid
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
from dispatcher.collector_manager import CollectorManager

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

    def __init__(self, collector_stub=None, db_path="dispatcher/tasks.db"):
        # 1) Initialize the task manager for persistent task record
        self.task_manager = TaskManager(db_path=db_path)
        # 2) Initialize the collector manager for collector registration, login, management, and load balancing
        self.collector_manager = CollectorManager()
        # 3) load the source catalog
        self.sources = load_sources("dispatcher/sources.json")

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
        # 1) Normalize the categories and locations
        task_cats = [c.strip() for c in request.categories.split(',') if c.strip()]
        task_locs = [l.strip() for l in request.location.split(',') if l.strip()]
        task_keywords = request.keywords.strip()

        # 2) Convert proto timestamps to ISO strings + UNIX expiry (easy expiry checking)
        dt_start = request.start_time.ToDatetime().astimezone(datetime.timezone.utc)
        dt_end = request.end_time.ToDatetime().astimezone(datetime.timezone.utc)
        start_iso = dt_start.isoformat()
        end_iso = dt_end.isoformat()
        end_ts = dt_end.timestamp()


        # 3) Create task ID and match sources to task from catalog
        task_id = str(uuid.uuid4())
        matched = match_sources(task_cats, task_locs, self.sources)
        if not matched:
            return TaskStartResponse(
                success=False,
                message=f"No sources matched for categories={task_cats} and locations={task_locs} (task_id={task_id})"
            )
        print(f"Dispatching {len(matched)} sources for task {task_id}")

        # 4) Persist new task in PENDING state
        self.task_manager.create_task(
            task_id = task_id,
            token = request.token,
            keywords = task_keywords,
            categories = task_cats,
            locations = task_locs,
            start_time = start_iso,
            end_time = end_iso
        )


        # 5) Assign each source (or batch) to least loaded collector
        assigned = []
        failed = []
        for src in matched:
            ok, msg = self.collector_manager.assign_task_balanced(
                task_id, [src["id"]], end_ts
            )
            if ok:
                assigned.append(src["id"])
            else:
                failed.append(src["id"])
                print(f"[WARN] Failed to assign source {src['id']} to task {task_id}: {msg}")

        # 6) Evaluate success and return response
        if assigned:
            # mark task as dispatched
            self.task_manager.mark_dispatched(task_id)
        else:
            # mark task as failed since nothing assigned
            self.task_manager.mark_failed(task_id)
            return TaskStartResponse(
                success=False,
                message=f"Task failed to assign any sources (task_id={task_id})"
            )
        
        # Partial or full success
        msg = (f"Task {task_id} started; assigned {len(assigned)}/{len(matched)} sources")
        if failed:
            msg += "; failed to assign: " + ", ".join(failed)

        return TaskStartResponse(
            success=True,
            message=msg,
            task_id=task_id
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