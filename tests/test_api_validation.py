import pytest
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_create_workflow_rejects_unknown_dependency(client):
    resp = client.post("/api/v1/workflows", json=[
        {"name": "a", "dependencies": ["does-not-exist"]},
    ])
    assert resp.status_code == 400
    assert "unknown task" in resp.json()["detail"]


def test_create_workflow_rejects_duplicate_task_name(client):
    resp = client.post("/api/v1/workflows", json=[
        {"name": "a"},
        {"name": "a"},
    ])
    assert resp.status_code == 400
    assert "duplicate task name" in resp.json()["detail"]


def test_create_workflow_rejects_missing_name(client):
    resp = client.post("/api/v1/workflows", json=[{"dependencies": []}])
    assert resp.status_code == 422


def test_create_workflow_accepts_valid_definition(client):
    resp = client.post("/api/v1/workflows", json=[
        {"name": "extract"},
        {"name": "transform", "dependencies": ["extract"]},
    ])
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_tasks"] == 2
    assert len(body["execution_order"]) == 2


def test_dashboard_page_serves_regardless_of_cwd(client):
    resp = client.get("/dashboard/some-workflow-id")
    assert resp.status_code == 200
    assert b"Workflow Dashboard" in resp.content
