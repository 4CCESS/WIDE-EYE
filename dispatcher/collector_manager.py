"""
CollectorManager: in‐memory registry of collectors, their heartbeats,
task assignments, and failover logic.
"""

import threading, time
from typing import Dict, List, Optional, Tuple, Any


class CollectorInfo:
    """
    Holds runtime information about one collector.
    """
    def __init__(self, name: str, secret: str):
        self.name: str = name
        self.secret: str = secret
        self.token: Optional[str] = None
        self.last_heartbeat: Optional[float] = None
        self.assigned_tasks: Dict[str, Dict[str, Any]] = {}
        self.tasks_assigned_count: int = 0
        self.tasks_completed_count: int = 0
        self.heartbeat_count: int = 0
        self.last_result_time: Optional[float] = None

    def is_authenticated(self, token: str) -> bool:
        return self.token == token

    def record_heartbeat(self, timestamp: Optional[float] = None) -> None:
        now = timestamp or time.time()
        self.last_heartbeat = now
        self.heartbeat_count += 1

    def assign_task(self, task_id: str, sources: List[str], end_time: float) -> None:
        """
        Add or update a task assignment for this collector.
        """
        if task_id in self.assigned_tasks:
            existing = self.assigned_tasks[task_id]["sources"]
            merged = existing + [s for s in sources if s not in existing]
            self.assigned_tasks[task_id]["sources"] = merged
        else:
            self.assigned_tasks[task_id] = {"sources": list(sources), "end_time": end_time}
            self.tasks_assigned_count += 1

    def record_task_result(self, task_id: str, timestamp: Optional[float] = None) -> None:
        now = timestamp or time.time()
        self.tasks_completed_count += 1
        self.last_result_time = now

    def get_tasks(self) -> Dict[str, Dict[str, Any]]:
        return {tid: dat.copy() for tid, dat in self.assigned_tasks.items()}

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "assigned_count": self.tasks_assigned_count,
            "completed_count": self.tasks_completed_count,
            "heartbeat_count": self.heartbeat_count,
            "last_heartbeat": self.last_heartbeat,
            "last_result_time": self.last_result_time,
            "current_load": len(self.assigned_tasks),
        }


class CollectorManager:
    """
    Thread-safe manager for CollectorInfo objects, assignment, and failover.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._collectors: Dict[str, CollectorInfo] = {}
        self._tokens: Dict[str, str] = {}

    def register_collector(self, name: str, secret: str) -> Tuple[bool, str]:
        """
        Register a new collector name/secret.
        """
        with self._lock:
            if name in self._collectors:
                return False, "Collector already registered"
            self._collectors[name] = CollectorInfo(name, secret)
            return True, "Collector registered"

    def login_collector(self, name: str, secret: str) -> Tuple[bool, Optional[str], str]:
        """
        Authenticate collector and issue session token.
        """
        import uuid
        with self._lock:
            info = self._collectors.get(name)
            if not info or info.secret != secret:
                return False, None, "Invalid name or secret"
            token = uuid.uuid4().hex
            info.token = token
            info.record_heartbeat()
            self._tokens[token] = name
            return True, token, "Login successful"

    def heartbeat(self, token: str, timestamp: Optional[float] = None) -> Tuple[bool, str]:
        """
        Record a heartbeat; returns False if token invalid.
        """
        with self._lock:
            name = self._tokens.get(token)
            if not name or name not in self._collectors:
                return False, "Invalid token"
            self._collectors[name].record_heartbeat(timestamp)
            return True, "Heartbeat recorded"

    def choose_least_loaded_collector(self, max_idle: float) -> Optional[CollectorInfo]:
        """
        Return the active collector with fewest tasks.
        """
        now = time.time()
        with self._lock:
            candidates = [
                c for c in self._collectors.values()
                if c.last_heartbeat and now - c.last_heartbeat <= max_idle
            ]
        if not candidates:
            return None
        return min(candidates, key=lambda c: len(c.assigned_tasks))

    def assign_task_to_collector(
        self, token: str, task_id: str, sources: List[str], end_time: float
    ) -> Tuple[bool, str]:
        """
        Assign a task to the collector identified by token.
        """
        with self._lock:
            name = self._tokens.get(token)
            if not name:
                return False, "Invalid token"
            self._collectors[name].assign_task(task_id, sources, end_time)
            return True, f"Task {task_id} assigned to {name}"

    def assign_task_balanced(
        self, task_id: str, sources: List[str], end_time: float, max_idle: float
    ) -> Tuple[bool, str]:
        """
        Choose least-loaded active collector and assign the task.
        """
        info = self.choose_least_loaded_collector(max_idle)
        if not info or not info.token:
            return False, "No available collectors"
        return self.assign_task_to_collector(info.token, task_id, sources, end_time)

    def record_task_result(
        self, token: str, task_id: str, timestamp: Optional[float] = None
    ) -> Tuple[bool, str]:
        """
        Record that a collector submitted a result.
        """
        with self._lock:
            name = self._tokens.get(token)
            info = self._collectors.get(name) if name else None
            if not info:
                return False, "Invalid token"
            info.record_task_result(task_id, timestamp)
            return True, f"Result for {task_id} recorded"

    def get_collector_info(self, name: str) -> Optional[CollectorInfo]:
        """
        Return the CollectorInfo by name.
        """
        with self._lock:
            return self._collectors.get(name)

    def get_all_collectors(self) -> List[CollectorInfo]:
        with self._lock:
            return list(self._collectors.values())

    def has_task_expired(self, task_id: str) -> bool:
        """
        True if any collector reports this task expired.
        """
        now = time.time()
        with self._lock:
            for info in self._collectors.values():
                data = info.assigned_tasks.get(task_id)
                if data and now >= data["end_time"]:
                    return True
        return False

    def purge_expired_tasks(self) -> List[Tuple[str, str]]:
        """
        Remove all expired assignments and return list of (collector,task_id).
        """
        now = time.time()
        expired = []
        with self._lock:
            for name, info in self._collectors.items():
                for tid, data in list(info.assigned_tasks.items()):
                    if now >= data["end_time"]:
                        expired.append((name, tid))
                        del info.assigned_tasks[tid]
        return expired

    def failover_dead_collectors(self, heartbeat_timeout: float) -> List[Tuple[str, str, str]]:
        """
        Detect collectors with stale heartbeats (>timeout*2), remove them,
        and return list of (dead_collector, task_id, reassigned_to).
        """
        now = time.time()
        results = []
        with self._lock:
            dead = [
                name for name, info in self._collectors.items()
                if info.last_heartbeat and now - info.last_heartbeat > heartbeat_timeout * 2
            ]
            for name in dead:
                info = self._collectors.pop(name)
                # Remove its token
                self._tokens = {t: n for t, n in self._tokens.items() if n != name}
                # Reassign its tasks
                for tid, data in info.assigned_tasks.items():
                    ok, msg = self.assign_task_balanced(
                        tid, data["sources"], data["end_time"], heartbeat_timeout
                    )
                    if ok:
                        results.append((name, tid, msg.split()[-1]))
        return results
