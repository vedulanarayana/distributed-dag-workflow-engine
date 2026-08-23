from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from typing import Dict, List
import random
import time

from app.dag.graph import DAG
from app.dag.topological_sort import KahnTopologicalSort
from app.state.state_manager import StateManager
from app.state.wal import WriteAheadLog
from app.retry.retry_manager import RetryManager
from app.worker.dispatcher import WorkerDispatcher
from app.config import STATE_DB, WAL_FILE

wal = WriteAheadLog(str(WAL_FILE))
state_manager = StateManager(STATE_DB, wal)
retry_manager = RetryManager(state_manager)
dispatcher = WorkerDispatcher(state_manager, retry_manager)

workflows: Dict[str, DAG] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    for workflow_id, definition in state_manager.load_all_workflows().items():
        workflows[workflow_id] = DAG.from_dict(definition)
    yield


app = FastAPI(title="Workflow Orchestration Engine", lifespan=lifespan)


def default_task_executor(node):
    time.sleep(random.uniform(0.1, 0.5))
    if random.random() < 0.1:
        raise RuntimeError("task failed")
    return True


@app.post("/api/v1/workflows")
async def create_workflow(tasks: List[Dict]):
    dag = DAG()
    id_map = {}
    for t in tasks:
        node = dag.add_node(t["name"])
        id_map[t["name"]] = node.task_id
    for t in tasks:
        for dep_name in t.get("dependencies", []):
            dag.nodes[id_map[t["name"]]].dependencies.append(id_map[dep_name])

    try:
        dag.validate()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    workflows[dag.workflow_id] = dag
    state_manager.save_workflow(dag.workflow_id, dag.to_dict())
    return {"workflow_id": dag.workflow_id, "total_tasks": len(dag.nodes),
            "execution_order": KahnTopologicalSort.sort(dag)}


@app.post("/api/v1/workflows/{workflow_id}/execute")
async def execute_workflow(workflow_id: str):
    dag = workflows.get(workflow_id)
    if not dag:
        raise HTTPException(status_code=404, detail="workflow not found")
    return await dispatcher.execute_workflow(dag, default_task_executor)


@app.get("/api/v1/workflows/{workflow_id}/status")
async def get_status(workflow_id: str):
    dag = workflows.get(workflow_id)
    if not dag:
        raise HTTPException(status_code=404, detail="workflow not found")
    nodes = []
    for task_id, node in dag.nodes.items():
        state = state_manager.get_state(task_id)
        nodes.append({"task_id": task_id, "name": node.name,
                       "state": state["state"] if state else "pending",
                       "retry_count": state["retry_count"] if state else 0})
    return {"workflow_id": workflow_id, "nodes": nodes}


@app.get("/api/v1/dashboard/data/{workflow_id}")
async def dashboard_data(workflow_id: str):
    dag = workflows.get(workflow_id)
    if not dag:
        raise HTTPException(status_code=404, detail="workflow not found")
    levels = KahnTopologicalSort.get_execution_levels(dag)
    nodes = []
    for task_id, node in dag.nodes.items():
        state = state_manager.get_state(task_id)
        level = next(i for i, l in enumerate(levels) if task_id in l)
        nodes.append({"id": task_id, "name": node.name, "dependencies": node.dependencies,
                       "state": state["state"] if state else "pending",
                       "retry_count": state["retry_count"] if state else 0, "level": level})
    return {"workflow_id": workflow_id, "nodes": nodes, "levels": levels}


@app.get("/dashboard/{workflow_id}", response_class=HTMLResponse)
async def dashboard(workflow_id: str):
    return HTMLResponse(open("app/dashboard/static/index.html").read())


@app.get("/api/v1/health")
async def health():
    return {"status": "ok", "workflows_loaded": len(workflows)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
