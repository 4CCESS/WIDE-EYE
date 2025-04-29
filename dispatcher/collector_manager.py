import threading
import uuid
import time
from typing import Dict, List, Optional, Any, Tuple


class CollectorInfo:
    """
    Holds runtime information about a registered collector, including authentication,
    heartbeat timestamps, assigned tasks (with expiration), and performance metrics.
    """
    def __init__(self, name: str, secret: str):
        """
        Initialize a new CollectorInfo.

        Args:
            name: Unique identifier for the collector.
            secret: Pre-shared secret for authentication.
        """
        self.name: str = name
        self.secret: str = secret
        self.token: Optional[str] = None
        self.last_heartbeat: Optional[float] = None

        # Task assignments with their expiration:
        # task_id -> {'sources': List[str], 'end_time': float}
        self.assigned_tasks: Dict[str, Dict[str, Any]] = {}

        # Performance metrics
        self.tasks_assigned_count: int = 0
        self.tasks_completed_count: int = 0
        self.heartbeat_count: int = 0
        self.last_result_time: Optional[float] = None

    def is_authenticated(self, token: str) -> bool:
        """
        Check if the provided token matches the collector's session token.

        Args:
            token: Session token to validate.

        Returns:
            True if tokens match, False otherwise.
        """
        return self.token == token

    def record_heartbeat(self, timestamp: Optional[float] = None) -> None:
        """
        Record a heartbeat from the collector, updating timestamp and count.

        Args:
            timestamp: Optional UNIX timestamp; if None, uses current time.
        """
        now = timestamp if timestamp is not None else time.time()
        self.last_heartbeat = now
        self.heartbeat_count += 1

    def assign_task(self, task_id: str, sources: List[str], end_time: float) -> None:
        """
        Assign a new task to this collector with a list of sources and expiration.

        Args:
            task_id: Unique identifier for the task.
            sources: List of source IDs the collector should process.
            end_time: UNIX timestamp when this task expires.
        """
        self.assigned_tasks[task_id] = {
            'sources': sources,
            'end_time': end_time
        }
        self.tasks_assigned_count += 1

    def record_task_result(self, task_id: str, timestamp: Optional[float] = None) -> None:
        """
        Record that a task result has been submitted by the collector.
        Does not remove the task assignment.

        Args:
            task_id: Unique identifier for the task.
            timestamp: Optional UNIX timestamp of result; if None, uses current time.
        """
        now = timestamp if timestamp is not None else time.time()
        self.tasks_completed_count += 1
        self.last_result_time = now

    def is_task_expired(self, task_id: str, now: Optional[float] = None) -> bool:
        """
        Check whether a specific task has passed its expiration time.

        Args:
            task_id: Identifier of the task to check.
            now: Optional current time; if None, uses current time.

        Returns:
            True if the task is expired or not assigned, False otherwise.
        """
        data = self.assigned_tasks.get(task_id)
        if not data:
            return False
        current = now if now is not None else time.time()
        return current >= data['end_time']

    def get_tasks(self) -> Dict[str, Dict[str, Any]]:
        """
        Retrieve a copy of all current task assignments with their details.

        Returns:
            A dictionary mapping task_id to its sources and end_time.
        """
        return {tid: dat.copy() for tid, dat in self.assigned_tasks.items()}

    def get_metrics(self) -> Dict[str, Any]:
        """
        Compile performance metrics for this collector.

        Returns:
            Dictionary containing assigned_count, completed_count, heartbeat_count,
            last_heartbeat, last_result_time, and current_load.
        """
        return {
            'name': self.name,
            'assigned_count': self.tasks_assigned_count,
            'completed_count': self.tasks_completed_count,
            'heartbeat_count': self.heartbeat_count,
            'last_heartbeat': self.last_heartbeat,
            'last_result_time': self.last_result_time,
            'current_load': len(self.assigned_tasks)
        }


class CollectorManager:
    """
    Manages collectors: registration, authentication, heartbeats, task assignment,
    load-balancing, task expiration, and metrics. All data stored in-memory.
    """
    def __init__(self) -> None:
        """
        Initialize a new CollectorManager with empty registries and locks.
        """
        self._lock = threading.Lock()
        self._collectors: Dict[str, CollectorInfo] = {}
        self._tokens: Dict[str, str] = {}

    def register_collector(self, name: str, secret: str) -> Tuple[bool, str]:
        """
        Register a new collector by name and secret.

        Args:
            name: Unique collector name.
            secret: Pre-shared secret for authentication.

        Returns:
            Tuple of (success, message).
        """
        with self._lock:
            if name in self._collectors:
                return False, f"Collector '{name}' is already registered."
            self._collectors[name] = CollectorInfo(name, secret)
            return True, f"Collector '{name}' registered successfully."

    def login_collector(self, name: str, secret: str) -> Tuple[bool, Optional[str], str]:
        """
        Authenticate a registered collector and issue a session token.

        Args:
            name: Collector name.
            secret: Authentication secret.

        Returns:
            Tuple of (success, token, message).
        """
        with self._lock:
            info = self._collectors.get(name)
            if not info or info.secret != secret:
                return False, None, "Invalid collector name or secret."
            token = uuid.uuid4().hex
            info.token = token
            info.record_heartbeat()
            self._tokens[token] = name
            return True, token, "Login successful."

    def heartbeat(self, token: str, timestamp: Optional[float] = None) -> Tuple[bool, str]:
        """
        Process a heartbeat from a collector to mark it active.

        Args:
            token: Session token of the collector.
            timestamp: Optional heartbeat time; if None, uses current time.

        Returns:
            Tuple of (success, message).
        """
        with self._lock:
            name = self._tokens.get(token)
            if not name or name not in self._collectors:
                return False, "Invalid or expired token."
            self._collectors[name].record_heartbeat(timestamp)
            return True, "Heartbeat recorded."

    def choose_least_loaded_collector(
        self,
        max_idle: float = 60.0
    ) -> Optional[CollectorInfo]:
        """
        Select the collector with the fewest active tasks and a recent heartbeat.

        Args:
            max_idle: Maximum seconds since last heartbeat to consider active.

        Returns:
            CollectorInfo of chosen collector or None if none available.
        """
        now = time.time()
        candidates: List[CollectorInfo] = []
        with self._lock:
            for info in self._collectors.values():
                if info.last_heartbeat and (now - info.last_heartbeat) <= max_idle:
                    candidates.append(info)
        if not candidates:
            return None
        return min(candidates, key=lambda c: len(c.assigned_tasks))

    def assign_task_to_collector(
        self,
        token: str,
        task_id: str,
        sources: List[str],
        end_time: float
    ) -> Tuple[bool, str]:
        """
        Assign a specific task to a collector identified by token.

        Args:
            token: Collector session token.
            task_id: Unique task ID.
            sources: List of source IDs.
            end_time: Expiration timestamp for the task.

        Returns:
            Tuple of (success, message).
        """
        with self._lock:
            name = self._tokens.get(token)
            if not name:
                return False, "Invalid or expired token."
            self._collectors[name].assign_task(task_id, sources, end_time)
            return True, f"Task '{task_id}' assigned to '{name}'."

    def assign_task_balanced(
        self,
        task_id: str,
        sources: List[str],
        end_time: float
    ) -> Tuple[bool, str]:
        """
        Auto-assign a task to the least-loaded active collector.

        Args:
            task_id: Unique task ID.
            sources: List of source IDs.
            end_time: Expiration timestamp for the task.

        Returns:
            Tuple of (success, message).
        """
        info = self.choose_least_loaded_collector()
        if not info or not info.token:
            return False, "No available collectors to assign task."
        return self.assign_task_to_collector(info.token, task_id, sources, end_time)

    def record_task_result(
        self,
        token: str,
        task_id: str,
        timestamp: Optional[float] = None
    ) -> Tuple[bool, str]:
        """
        Record that a collector has submitted a task result.

        Args:
            token: Collector session token.
            task_id: Unique task ID.
            timestamp: Optional result time; if None, uses current time.

        Returns:
            Tuple of (success, message).
        """
        with self._lock:
            name = self._tokens.get(token)
            if not name or name not in self._collectors:
                return False, "Invalid or expired token."
            self._collectors[name].record_task_result(task_id, timestamp)
            return True, f"Result for task '{task_id}' recorded."

    def get_collector_info(self, name: str) -> Optional[CollectorInfo]:
        """
        Retrieve the CollectorInfo object for a given collector by name.

        Args:
            name: Collector name.

        Returns:
            CollectorInfo or None if not found.
        """
        with self._lock:
            return self._collectors.get(name)

    def get_all_collectors(self) -> List[CollectorInfo]:
        """
        Get a list of all registered collectors, regardless of status.

        Returns:
            List of CollectorInfo objects.
        """
        with self._lock:
            return list(self._collectors.values())

    def get_tasks_for_collector(
        self,
        name: str
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        Get current task assignments for a specific collector.

        Args:
            name: Collector name.

        Returns:
            Dict mapping task_id to sources and end_time, or None if not found.
        """
        with self._lock:
            info = self._collectors.get(name)
            return info.get_tasks() if info else None

    def get_collector_metrics(
        self,
        name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve performance and activity metrics for a collector.

        Args:
            name: Collector name.

        Returns:
            Dict of metrics or None if not found.
        """
        with self._lock:
            info = self._collectors.get(name)
            return info.get_metrics() if info else None

    def purge_expired_tasks(self) -> List[Tuple[str, str]]:
        """
        Remove all task assignments that have passed their end_time.

        Returns:
            List of tuples (collector_name, task_id) for each expired assignment.
        """
        now = time.time()
        expired: List[Tuple[str, str]] = []
        with self._lock:
            for name, info in self._collectors.items():
                for task_id, data in list(info.assigned_tasks.items()):
                    if now >= data['end_time']:
                        expired.append((name, task_id))
                        del info.assigned_tasks[task_id]
        return expired

    def has_task_expired(self, task_id: str) -> bool:
        """
        Determine whether a specific task has expired across all collectors.

        Args:
            task_id: Unique task ID.

        Returns:
            True if expired, False otherwise.
        """
        now = time.time()
        with self._lock:
            for info in self._collectors.values():
                data = info.assigned_tasks.get(task_id)
                if data and now >= data['end_time']:
                    return True
        return False


# Example usage
if __name__ == "__main__":
    manager = CollectorManager()
    success, msg = manager.register_collector("col1", "secret")
    print(success, msg)
    ok, token, msg = manager.login_collector("col1", "secret")
    if ok:
        end_ts = time.time() + 120  # expires in 2 minutes
        success, msg = manager.assign_task_balanced("task123", ["sourceA", "sourceB"], end_ts)
        print(success, msg)
        print("Expired?", manager.has_task_expired("task123"))
        time.sleep(130)
        print("Purge:", manager.purge_expired_tasks())
        print("Expired now?", manager.has_task_expired("task123"))
