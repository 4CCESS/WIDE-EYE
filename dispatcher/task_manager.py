# dispatcher/task_manager.py

import sqlite3
import json
import datetime
from typing import List, Optional, Dict, Tuple, Any

class TaskManager:
    """Persistent store for TaskRequest metadata and status."""

    def __init__(self, db_path: str = "dispatcher/tasks.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
              task_id      TEXT PRIMARY KEY,
              token        TEXT    NOT NULL,
              keywords     TEXT    NOT NULL,
              categories   TEXT    NOT NULL,
              locations    TEXT    NOT NULL,
              start_time   TEXT    NOT NULL,
              end_time     TEXT    NOT NULL,
              status       TEXT    NOT NULL,
              created_at   TEXT    NOT NULL,
              updated_at   TEXT    NOT NULL
            )
        """)
        self.conn.commit()

    def _now(self) -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def create_task(self,
                    task_id: str,
                    token: str,
                    keywords: str,
                    categories: List[str],
                    locations: List[str],
                    start_time: str,
                    end_time: str) -> None:
        """Insert a new task in PENDING state."""
        now = self._now()
        self.conn.execute("""
            INSERT INTO tasks
            (task_id, token, keywords, categories, locations, start_time, end_time, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
        """, (
            task_id,
            token,
            keywords,
            json.dumps(categories),
            json.dumps(locations),
            start_time,
            end_time,
            now,
            now
        ))
        self.conn.commit()

    def update_status(self, task_id: str, new_status: str) -> None:
        """Change the task’s status (e.g. DISPATCHED, COMPLETED, FAILED)."""
        now = self._now()
        self.conn.execute("""
            UPDATE tasks
            SET status = ?, updated_at = ?
            WHERE task_id = ?
        """, (new_status, now, task_id))
        self.conn.commit()

    def mark_dispatched(self, task_id: str) -> None:
        self.update_status(task_id, "DISPATCHED")

    def mark_completed(self, task_id: str) -> None:
        self.update_status(task_id, "COMPLETED")

    def mark_failed(self, task_id: str) -> None:
        self.update_status(task_id, "FAILED")

    def cancel_task(self, task_id: str) -> None:
        """Soft-cancel a task."""
        self.update_status(task_id, "CANCELLED")

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single task by ID, or None if not found."""
        row = self.conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        if not row:
            return None
        return {
            "task_id":    row[0],
            "token":      row[1],
            "keywords":   row[2],
            "categories": json.loads(row[3]),
            "locations":  json.loads(row[4]),
            "start_time": row[5],
            "end_time":   row[6],
            "status":     row[7],
            "created_at": row[8],
            "updated_at": row[9],
        }

    def list_tasks(self,
                   token: Optional[str] = None,
                   statuses: Optional[List[str]] = None,
                   time_range: Optional[Tuple[str, str]] = None,
                   limit: Optional[int] = None,
                   offset: Optional[int] = None
                  ) -> List[Dict[str, Any]]:
        """
        List tasks, optionally filtered by:
          - token
          - one or more statuses
          - start_time between time_range[0] and time_range[1]
        Supports pagination via limit/offset.
        """
        sql = ["SELECT * FROM tasks"]
        args: List[Any] = []

        clauses = []
        if token:
            clauses.append("token = ?")
            args.append(token)
        if statuses:
            clauses.append(
                "status IN ({})".format(",".join("?"*len(statuses)))
            )
            args.extend(statuses)
        if time_range:
            clauses.append("start_time >= ? AND start_time <= ?")
            args.extend([time_range[0], time_range[1]])

        if clauses:
            sql.append("WHERE " + " AND ".join(clauses))
        sql.append("ORDER BY created_at DESC")

        if limit is not None:
            sql.append("LIMIT ?")
            args.append(limit)
        if offset is not None:
            sql.append("OFFSET ?")
            args.append(offset)

        cursor = self.conn.execute(" ".join(sql), tuple(args))
        rows = cursor.fetchall()
        tasks = []
        for row in rows:
            tasks.append({
                "task_id":    row[0],
                "token":      row[1],
                "keywords":   row[2],
                "categories": json.loads(row[3]),
                "locations":  json.loads(row[4]),
                "start_time": row[5],
                "end_time":   row[6],
                "status":     row[7],
                "created_at": row[8],
                "updated_at": row[9],
            })
        return tasks

    def list_tasks_by_status(self, statuses: List[str]) -> List[Dict[str, Any]]:
        """Shortcut for listing by status."""
        return self.list_tasks(statuses=statuses)

    def list_pending_or_dispatched(self) -> List[Dict[str, Any]]:
        """
        For resuming on startup: tasks still PENDING or DISPATCHED.
        """
        return self.list_tasks(statuses=["PENDING", "DISPATCHED"])

    def count_tasks(self,
                    statuses: Optional[List[str]] = None
                   ) -> int:
        """
        Return the number of tasks, optionally filtered by status.
        """
        if statuses:
            sql = "SELECT COUNT(*) FROM tasks WHERE status IN ({})".format(",".join("?"*len(statuses)))
            args = statuses
        else:
            sql = "SELECT COUNT(*) FROM tasks"
            args = []
        row = self.conn.execute(sql, tuple(args)).fetchone()
        return row[0] if row else 0
