"""
Dispatcher gRPC server: handles clients and collectors, persistent tasks,
user management, and failover logic.
"""

import grpc, time, datetime, traceback, json, uuid, threading, logging
from typing import Dict
from concurrent import futures
from collections import defaultdict

from google.protobuf.timestamp_pb2 import Timestamp
from proto.dispatcher_pb2 import (
    RegisterResponse, LoginResponse,
    TaskStartResponse, TaskResult,
    ListCategoriesResponse, ListLocationsResponse,
    CollectorRegisterResponse, CollectorLoginResponse,
    HeartbeatResponse, TaskAssignment, CollectorTaskResultAck
)
from proto.dispatcher_pb2_grpc import (
    ClientDispatcherServicer, add_ClientDispatcherServicer_to_server,
    CollectorDispatcherServicer, add_CollectorDispatcherServicer_to_server
)

from dispatcher.config import DISPATCHER_CONFIG
from dispatcher.user_manager import UserManager
from dispatcher.task_manager import TaskManager
from dispatcher.collector_manager import CollectorManager
from dispatcher.source_catalog import load_sources, list_available_categories, list_available_locations, match_sources

# --- Logging Setup ---
logger = logging.getLogger("Dispatcher")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(DISPATCHER_CONFIG["log_file"])
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)


def grpc_safe(f):
    """
    Decorator: wrap RPC methods to catch exceptions and return gRPC INTERNAL.
    """
    def wrapper(self, request, context):
        try:
            return f(self, request, context)
        except Exception as e:
            logger.error(f"Exception in {f.__name__}: {e}")
            traceback.print_exc()
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return None
    return wrapper


class DispatcherService(ClientDispatcherServicer):
    """
    Client-facing service: Register, Login, StartTask, StreamResults,
    ListAvailableCategories/Locations.
    """
    def __init__(self, tm: TaskManager, cm: CollectorManager, sources, results, results_cond):
        self.task_manager = tm
        self.collector_manager = cm
        self.sources = sources
        self._results = results
        self._results_cond = results_cond
        self.user_manager = UserManager()
        self._user_tokens: Dict[str, str] = {}  # token -> username

    @grpc_safe
    def Register(self, request, context):
        """
        Register a new client user.
        """
        ok, msg = self.user_manager.register_user(request.username, request.password)
        logger.info(f"Register({request.username}) -> {ok}")
        return RegisterResponse(success=ok, message=msg)

    @grpc_safe
    def Login(self, request, context):
        """
        Authenticate a user and issue a session token.
        """
        if not self.user_manager.authenticate_user(request.username, request.password):
            logger.warning(f"Login failed for {request.username}")
            return LoginResponse(success=False, message="Invalid credentials", token="")
        token = uuid.uuid4().hex
        self._user_tokens[token] = request.username
        logger.info(f"Login successful for {request.username}, token={token}")
        return LoginResponse(success=True, message="Login successful", token=token)

    @grpc_safe
    def StartTask(self, request, context):
        """
        Create a new task, match sources, persist, and dispatch to collectors.
        """
        user = self._user_tokens.get(request.token)
        if not user:
            return TaskStartResponse(success=False, message="Authentication required")

        cats = [c.strip() for c in request.categories.split(",") if c.strip()]
        locs = [l.strip() for l in request.location.split(",") if l.strip()]
        kw = request.keywords.strip()

        # Convert timestamps
        dt_start = request.start_time.ToDatetime().replace(tzinfo=datetime.timezone.utc)
        dt_end = request.end_time.ToDatetime().replace(tzinfo=datetime.timezone.utc)
        iso_start, iso_end = dt_start.isoformat(), dt_end.isoformat()
        ts_end = dt_end.timestamp()

        task_id = uuid.uuid4().hex
        matched = match_sources(cats, locs, self.sources)
        if not matched:
            return TaskStartResponse(
                success=False,
                message=f"No sources for {cats}/{locs}"
            )
        self.task_manager.create_task(task_id, request.token, kw, cats, locs, iso_start, iso_end)

        assigned, failed = [], []
        for src in matched:
            ok, msg = self.collector_manager.assign_task_balanced(
                task_id, [src["id"]], ts_end, DISPATCHER_CONFIG["heartbeat_timeout"]
            )
            if ok:
                assigned.append(src["id"])
            else:
                failed.append(src["id"])
                logger.warning(f"Assign fail: {src['id']} -> {msg}")

        if assigned:
            self.task_manager.mark_dispatched(task_id)
            logger.info(f"Task {task_id} dispatched to {len(assigned)} collectors")
            message = f"Assigned {len(assigned)}/{len(matched)} sources"
            if failed:
                message += "; failed: " + ",".join(failed)
            return TaskStartResponse(success=True, message=message, task_id=task_id)
        else:
            self.task_manager.mark_failed(task_id)
            return TaskStartResponse(success=False, message="No collectors available", task_id="")

    @grpc_safe
    def StreamResults(self, request, context):
        """
        Stream TaskResult protos back to the client until task completes.
        """
        task_id = request.task_id
        cond = self._results_cond[task_id]
        while True:
            with cond:
                cond.wait(timeout=1.0)
                while self._results[task_id]:
                    yield self._results[task_id].pop(0)
            task = self.task_manager.get_task(task_id)
            if task and task["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
                break

    @grpc_safe
    def ListAvailableCategories(self, request, context):
        """
        Reload sources.json and return unique categories.
        """
        self.sources = load_sources("dispatcher/sources.json")
        cats = list_available_categories(self.sources)
        return ListCategoriesResponse(categories=cats)

    @grpc_safe
    def ListAvailableLocations(self, request, context):
        """
        Reload sources.json and return unique locations.
        """
        locs = list_available_locations(self.sources)
        return ListLocationsResponse(locations=locs)


class CollectorDispatcherService(CollectorDispatcherServicer):
    """
    Collector-facing service: RegisterCollector, LoginCollector,
    Heartbeat, StreamTasks, SubmitTaskResult.
    """
    def __init__(self, tm, cm, sources, results, results_cond):
        self.task_manager = tm
        self.collector_manager = cm
        self.sources = sources
        self._results = results
        self._results_cond = results_cond

    @grpc_safe
    def RegisterCollector(self, request, context):
        ok, msg = self.collector_manager.register_collector(request.name, request.secret)
        logger.info(f"RegisterCollector({request.name}) -> {ok}")
        return CollectorRegisterResponse(success=ok, message=msg)

    @grpc_safe
    def LoginCollector(self, request, context):
        ok, token, msg = self.collector_manager.login_collector(request.name, request.secret)
        logger.info(f"LoginCollector({request.name}) -> {ok}, token={token}")
        return CollectorLoginResponse(success=ok, token=token or "", message=msg)

    @grpc_safe
    def Heartbeat(self, request, context):
        ts = request.timestamp.ToDatetime().timestamp()
        ok, msg = self.collector_manager.heartbeat(request.token, ts)
        logger.debug(f"Heartbeat(token={request.token}) -> {ok}")
        return HeartbeatResponse(success=ok, message=msg)

    @grpc_safe
    def StreamTasks(self, request, context):
        """
        Continuously yield TaskAssignment messages, purging expired tasks,
        and performing failover when necessary.
        """
        from dispatcher.config import DISPATCHER_CONFIG
        name = self.collector_manager._tokens.get(request.token)
        if not name:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid token")

        sent = set()
        while True:
            # 1) Purge expired tasks
            self.collector_manager.purge_expired_tasks()

            # 2) Detect dead collectors & reassign
            failures = self.collector_manager.failover_dead_collectors(
                DISPATCHER_CONFIG["heartbeat_timeout"]
            )
            for dead_coll, task_id, new_coll in failures:
                logger.warning(f"Collector '{dead_coll}' missed heartbeats; reassigned task {task_id} → {new_coll}")

            # 3) Stream each new assignment once
            info = self.collector_manager.get_collector_info(name)
            if not info:
                break
            for tid, data in info.get_tasks().items():
                if tid in sent:
                    continue
                task = self.task_manager.get_task(tid)
                ta = TaskAssignment(
                    task_id=tid,
                    keywords=task["keywords"],
                    category=(task["categories"][0] if task["categories"] else ""),
                    location=(task["locations"][0] if task["locations"] else ""),
                )
                # start_time
                st = Timestamp(); st.FromDatetime(datetime.datetime.fromisoformat(task["start_time"]))
                ta.start_time.CopyFrom(st)
                # end_time
                et = Timestamp(); et.FromDatetime(datetime.datetime.fromisoformat(task["end_time"]))
                ta.end_time.CopyFrom(et)
                ta.sources.extend(data["sources"])
                yield ta
                sent.add(tid)

            time.sleep(5)

    @grpc_safe
    def SubmitTaskResult(self, request, context):
        """
        Receive a CollectorTaskResult, record metrics,
        enqueue a TaskResult for client streaming, and notify.
        """
        ts = request.timestamp.ToDatetime().timestamp()
        ok, msg = self.collector_manager.record_task_result(request.token, request.task_id, ts)
        tr = TaskResult(task_id=request.task_id, result=request.result, timestamp=request.timestamp)
        cond = self._results_cond[request.task_id]
        with cond:
            self._results[request.task_id].append(tr)
            cond.notify_all()
        return CollectorTaskResultAck(success=ok, message=msg)


def start_expiry_sweeper(task_manager, result_conds, interval=5):
    """
    Mark tasks COMPLETE when their end_time passes; wake client streams.
    """
    def sweeper():
        while True:
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for task in task_manager.list_pending_or_dispatched():
                if task["end_time"] <= now_iso:
                    task_manager.mark_completed(task["task_id"])
                    with result_conds[task["task_id"]]:
                        result_conds[task["task_id"]].notify_all()
            time.sleep(interval)

    threading.Thread(target=sweeper, daemon=True).start()


def serve():
    """
    Initialize managers, services, and gRPC server.
    """
    # Shared managers & data
    task_manager = TaskManager(db_path=DISPATCHER_CONFIG["db_path"])
    collector_manager = CollectorManager()
    sources = load_sources("dispatcher/sources.json")
    results = defaultdict(list)
    result_conds = defaultdict(threading.Condition)

    # Services
    client_svc = DispatcherService(task_manager, collector_manager, sources, results, result_conds)
    coll_svc = CollectorDispatcherService(task_manager, collector_manager, sources, results, result_conds)

    start_expiry_sweeper(task_manager, result_conds)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ClientDispatcherServicer_to_server(client_svc, server)
    add_CollectorDispatcherServicer_to_server(coll_svc, server)

    server.add_insecure_port(f"[::]:{DISPATCHER_CONFIG['client_port']}")
    server.add_insecure_port(f"[::]:{DISPATCHER_CONFIG['collector_port']}")
    server.start()
    logger.info(f"gRPC server listening on {DISPATCHER_CONFIG['client_port']} (client) and {DISPATCHER_CONFIG['collector_port']} (collector)")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
