"""
OSINT Collector: registers/login, heartbeats, streams assignments,
fetches RSS entries, and submits results via gRPC.
"""

import grpc, time, threading, datetime, json, logging
import feedparser
from proto.dispatcher_pb2_grpc import CollectorDispatcherStub
from proto.dispatcher_pb2 import (
    CollectorRegisterRequest, CollectorLoginRequest,
    HeartbeatRequest, TaskStreamRequest, CollectorTaskResult
)
from collector.config import COLLECTOR_CONFIG

# Logging setup
logger = logging.getLogger("Collector")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(COLLECTOR_CONFIG["log_file"])
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)


class Collector:
    """
    Collector lifecycle: authenticate, heartbeat, stream tasks,
    collect per-source data, submit results.
    """

    def __init__(self):
        """
        Configure gRPC stub, intervals, and seen-entry tracking.
        """
        addr = f"{COLLECTOR_CONFIG['dispatcher_address']}:{COLLECTOR_CONFIG['dispatcher_port']}"
        logger.info(f"Connecting to collector service at {addr}")
        self.channel = grpc.insecure_channel(addr)
        self.stub = CollectorDispatcherStub(self.channel)

        self.token: str = ""
        self.heartbeat_interval = COLLECTOR_CONFIG["heartbeat_interval"]
        self.default_rss_refresh = COLLECTOR_CONFIG["rss_refresh"]
        self.data_source_methods = {"rss": self._collect_rss}
        self.seen = {}  # (task_id, source_url) -> set(entry_id)

    def run(self):
        """
        Start authentication, heartbeat thread, and assignment streaming.
        """
        self._authenticate()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._stream_tasks_loop()

    def _authenticate(self):
        """
        Prompt user to register or login; call the corresponding RPCs.
        """
        print("1) Register  2) Login")
        choice = input("Choice: ").strip()
        name = input("Collector name: ").strip()
        secret = input("Secret: ").strip()

        if choice == "1":
            resp = self.stub.RegisterCollector(
                CollectorRegisterRequest(name=name, secret=secret)
            )
            logger.info(f"RegisterCollector -> success={resp.success}, msg={resp.message}")

        login_resp = self.stub.LoginCollector(
            CollectorLoginRequest(name=name, secret=secret)
        )
        if not login_resp.success:
            logger.error(f"Login failed: {login_resp.message}")
            raise SystemExit(1)
        self.token = login_resp.token
        logger.info(f"Logged in, token={self.token}")

    def _heartbeat_loop(self):
        """
        Send HeartbeatRequest every configured interval.
        """
        while True:
            ts = datetime.datetime.utcnow()
            try:
                self.stub.Heartbeat(HeartbeatRequest(token=self.token, timestamp=ts))
                logger.debug("Heartbeat sent")
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            time.sleep(self.heartbeat_interval)

    def _stream_tasks_loop(self):
        """
        Block on StreamTasks RPC; spawn a thread per assignment.
        """
        for assignment in self.stub.StreamTasks(TaskStreamRequest(token=self.token)):
            logger.info(f"Received assignment: {assignment.task_id}, sources={assignment.sources}")
            threading.Thread(
                target=self._handle_task, args=(assignment,), daemon=True
            ).start()

    def _handle_task(self, assignment):
        """
        Wait until start_time, loop until end_time collecting each source.
        """
        tid = assignment.task_id
        start_dt = assignment.start_time.ToDatetime().replace(tzinfo=datetime.timezone.utc)
        end_dt = assignment.end_time.ToDatetime().replace(tzinfo=datetime.timezone.utc)
        for src in assignment.sources:
            self.seen[(tid, src)] = set()

        now = datetime.datetime.now(datetime.timezone.utc)
        if now < start_dt:
            wait = (start_dt - now).total_seconds()
            logger.info(f"Task {tid}: waiting {wait:.1f}s until {start_dt}")
            time.sleep(wait)

        logger.info(f"Task {tid}: collecting until {end_dt}")
        while datetime.datetime.now(datetime.timezone.utc) < end_dt:
            for src in assignment.sources:
                self.data_source_methods["rss"](tid, src)
            time.sleep(self.default_rss_refresh)
        logger.info(f"Task {tid}: complete")

    def _collect_rss(self, task_id: str, source_url: str):
        """
        Parse RSS, dedupe on entry.id/link, wrap payload in JSON,
        and call SubmitTaskResult for each new entry.
        """
        try:
            feed = feedparser.parse(source_url)
            if feed.bozo:
                logger.warning(f"RSS bozo: {feed.bozo_exception}")
                return
        except Exception as e:
            logger.error(f"RSS parse error ({source_url}): {e}")
            return

        key = (task_id, source_url)
        for entry in feed.entries:
            eid = getattr(entry, "id", entry.link)
            if eid in self.seen[key]:
                continue
            payload = {
                "task_id": task_id,
                "source": source_url,
                "entry_id": eid,
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", ""),
            }
            req = CollectorTaskResult(
                token=self.token,
                task_id=task_id,
                timestamp=datetime.datetime.utcnow(),
                result=json.dumps(payload)
            )
            try:
                ack = self.stub.SubmitTaskResult(req)
                if ack.success:
                    logger.info(f"Submitted {eid}")
                    self.seen[key].add(eid)
                else:
                    logger.warning(f"Submit failed: {ack.message}")
            except Exception as e:
                logger.error(f"Error submitting {eid}: {e}")


if __name__ == "__main__":
    Collector().run()
