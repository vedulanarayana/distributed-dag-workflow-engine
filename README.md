# distributed-dag-workflow-engine

A fault-tolerant, concurrent DAG workflow orchestration engine implementing Kahn's algorithm, SQLite Write-Ahead Logging (WAL) for state durability, and exponential-backoff retry policies.

## Features

- Define workflows as DAGs of named tasks with dependencies
- Kahn's algorithm for topological sort, cycle detection, and level-based execution planning
- Tasks within the same dependency level run concurrently
- Write-ahead log for every state transition, replayed on startup to recover from a crash mid-task
- Exponential backoff with jitter, configurable max retries, dead-letter table for tasks that exhaust retries
- Idempotency keys to prevent duplicate state transitions
- HTTP API (FastAPI) to submit, execute, and inspect workflows, plus a polling dashboard

## Running locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

Then:

```bash
curl -X POST localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '[{"name": "extract"}, {"name": "transform", "dependencies": ["extract"]}, {"name": "load", "dependencies": ["transform"]}]'
```

Grab the returned `workflow_id` and:

```bash
curl -X POST localhost:8080/api/v1/workflows/<workflow_id>/execute
curl localhost:8080/api/v1/workflows/<workflow_id>/status
```

Or open `http://localhost:8080/dashboard/<workflow_id>` in a browser to watch task states update live.

## Running with Docker

```bash
docker build -t workflow-engine .
docker run -p 8080:8080 workflow-engine
```

## Tests

```bash
pytest
```

Covers DAG validation (cycles, missing dependencies), retry/backoff behavior, dead-letter routing, idempotency, crash recovery via WAL replay, and concurrent execution of same-level tasks.

## Layout

```
app/
  config.py              paths, task states, retry constants
  main.py                FastAPI app and routes
  dag/
    graph.py              DAG and DAGNode
    topological_sort.py   Kahn's algorithm, execution levels
  state/
    wal.py                write-ahead log
    state_manager.py      SQLite-backed task state, dead letters, idempotency
  retry/
    backoff.py             exponential backoff with jitter
    retry_manager.py       retry loop tied into state transitions
  worker/
    dispatcher.py           runs each execution level concurrently
  dashboard/static/         polling dashboard UI
tests/
```
