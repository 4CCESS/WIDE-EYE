
# WIDE EYE

**COMPSCI 2620 Final Project**  
**Spring 2025**  
**Authors:** Jesse James & Ruben Valenzuela

---

## Overview

WIDE EYE is a lightweight, scalable Open-Source Intelligence (OSINT) system designed for distributed, task-driven collection and analysis of publicly-available information. It currently supports:

- **RSS/Atom feed ingestion**  
- **GDACS disaster alerts** (prototype)  
- **gRPC-based orchestration** of clients, dispatcher, and collectors  
- **Interactive Qt-based client** with map visualization and NER-powered geotagging  

The system is organized into three cooperating components:

1. **Client**: PySide6 GUI for user registration/login, task creation (filter by keywords, categories, locations, time range), and real-time display of results on a Folium map.  
2. **Dispatcher**: Central gRPC server that manages users, source catalog, tasks, and collectors; persists tasks in SQLite; performs load-balanced assignment; and handles heartbeat-driven failover.  
3. **Collector**: Stateless workers that register/login, heartbeat, receive `TaskAssignment`s, fetch data (RSS, GDACS), dedupe, and stream results back to the dispatcher.

Each component has its own JSON-based `config.json` to eliminate magic numbers and hard-coded paths, plus file-based logging and verbose in-code documentation.

---

## Architecture

```

```
┌────────┐          ┌─────────────┐          ┌───────────┐
│ Client │◀──gRPC──▶│ Dispatcher  │◀──gRPC──▶│ Collector │
└────────┘          └─────────────┘          └───────────┘
    ▲                                          ▲
    │                                          │
```

GUI: PySide6                              RSS/GDACS parsing
Map: Folium                              feedparser / aio-georss-gdacs
NLP: spaCy
Persistence: SQLite (tasks.db, users.db)

````

- **Client**  
  - `client/config.json` for address & ports  
  - `client/client.py` – main window, auth pages, task forms, map & results  
  - `client/multi_select_search_box.py` – searchable, multi-select dropdown  

- **Dispatcher**  
  - `dispatcher/config.json` for ports, DB paths, timeouts  
  - `dispatcher/user_manager.py` – secure user table (PBKDF2-SHA256)  
  - `dispatcher/task_manager.py` – persistent task metadata & status  
  - `dispatcher/collector_manager.py` – in-memory collector registry, heartbeat, failover  
  - `dispatcher/dispatcher.py` – gRPC servicers for clients & collectors  
  - `dispatcher/source_catalog.py` – load & match RSS source catalog  

- **Collector**  
  - `collector/config.json` for dispatcher address, intervals, log file  
  - `collector/collector.py` – gRPC-based RSS collector  
  - `collector/gdacs_collector.py` – asyncio/tcp-based GDACS prototype  

---

## Prerequisites

- **Python 3.10+**  
- **pip install**:  
  ```bash
  pip install \
    PySide6 folium spacy grpcio grpcio-tools protobuf \
    feedparser aiohttp aio-georss-gdacs
  ```

* **spaCy model** (run once):

  ```bash
  python -m spacy download en_core_web_sm
  ```
* **SQLite** (bundled with Python)

---

## Configuration

Each component directory contains a `config.json`. Review and edit these values before first run:

* **client/config.json**

  ```json
  {
    "dispatcher_address": "localhost",
    "dispatcher_port": 50051,
    "log_file": "client/client.log"
  }
  ```
* **dispatcher/config.json**

  ```json
  {
    "client_port": 50051,
    "collector_port": 50052,
    "db_path": "dispatcher/tasks.db",
    "user_db_path": "dispatcher/users.db",
    "heartbeat_timeout": 60,
    "log_file": "dispatcher/dispatcher.log"
  }
  ```
* **collector/config.json**

  ```json
  {
    "dispatcher_address": "localhost",
    "dispatcher_port": 50052,
    "heartbeat_interval": 30,
    "rss_refresh": 60,
    "log_file": "collector/collector.log"
  }
  ```

---

## Running the System

1. **Start the Dispatcher**

   ```bash
   cd dispatcher
   python dispatcher.py
   ```

   * Listens on ports `client_port` and `collector_port`.
   * Initializes or migrates `tasks.db` and `users.db`.

2. **Start One or More Collectors**

   ```bash
   cd collector
   python collector.py
   ```

   * Follow prompts to register/login.
   * Remains connected via heartbeat & streams assignments.

3. **Start the Client GUI**

   ```bash
   cd client
   python client.py
   ```

   * Register/Login, then create tasks.
   * Watch incoming results in the map + list.

---

## Testing

* **Unit & Integration Tests** (pytest):

  ```bash
  pytest                                 # from project root
  ```

  * Tests cover source catalog, task & user managers, collector manager, RPC flows, and client request construction.

* **Ad-hoc Connectivity Scripts**:

  * `collector_dispatcher_connection.py` – client script that starts a 2-minute task and prints streamed results.
  * `dispatcher_client_connection.py` – dummy dispatcher stub for client testing.

---

## Directory Structure

```
.
├── client
│   ├── client.py
│   ├── config.json
│   ├── config.py
│   ├── countries+states+cities.json
│   ├── multi_select_search_box.py
│   └── client.log
├── dispatcher
│   ├── dispatcher.py
│   ├── config.json
│   ├── config.py
│   ├── user_manager.py
│   ├── task_manager.py
│   ├── collector_manager.py
│   ├── source_catalog.py
│   ├── tasks.db
│   ├── users.db
│   └── dispatcher.log
├── collector
│   ├── collector.py
│   ├── gdacs_collector.py
│   ├── config.json
│   ├── config.py
│   └── collector.log
├── proto
│   └── *.proto / generated _pb2.py, _pb2_grpc.py
├── tests
│   └── test_*.py
└── README.md
```

---

## Notes & Future Work

* **GDACS collector** remains a prototype. You can integrate it into the gRPC pipeline once its proto definitions are finalized.
* **Additional data sources** (APIs, file watches, social media) can be added by extending `Collector.data_source_methods`.
* **UI Enhancements:**

  * Task cancellation, progress indicators, map clustering.
  * Custom styling & theme for PySide6.

Enjoy exploring the world through distributed OSINT!
— Jesse & Ruben
