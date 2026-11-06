"""The HTTP layer: status codes and response shapes. FastAPI's TestClient runs the app
in-process (including startup/shutdown), so no server is needed."""
import uuid

import pytest
from fastapi.testclient import TestClient

from quota.app import create_app
from quota.config import Settings
from tests.conftest import TEST_REDIS_URL, reset_state


@pytest.fixture
def client(test_db):
    reset_state(test_db)
    app = create_app(Settings(database_url=test_db, redis_url=TEST_REDIS_URL))
    with TestClient(app) as c:      # `with` runs the lifespan: pools open, features load
        yield c


def reserve(client, units, request_id=None, org=2, feature="feature1"):
    return client.post("/v1/reserve", json={"org_id": org, "feature": feature, "units": units,
                                            "request_id": request_id or uuid.uuid4().hex})


def test_reserve_commit_usage(client):
    r = reserve(client, 30, "req-1")
    assert r.status_code == 200
    assert r.json() == {"result": "reserved", "duplicate": False, "units": 30}

    r = client.post("/v1/commit", json={"org_id": 2, "request_id": "req-1"})
    assert r.json() == {"result": "committed", "duplicate": False}
    r = client.post("/v1/commit", json={"org_id": 2, "request_id": "req-1"})
    assert r.json() == {"result": "committed", "duplicate": True}

    u = client.get("/v1/usage/2/feature1").json()
    assert (u["used"], u["reserved"], u["limit"], u["remaining"]) == (30, 0, 100, 70)
    assert u["resets_at"].endswith("Z") or u["resets_at"].endswith("+00:00")


def test_quota_exceeded_is_429(client):
    assert reserve(client, 100).status_code == 200
    r = reserve(client, 1)
    assert r.status_code == 429
    assert r.json()["available"] == 0


def test_release_then_commit_conflicts(client):
    reserve(client, 5, "req-2")
    assert client.post("/v1/release", json={"org_id": 2, "request_id": "req-2"}).status_code == 200
    assert client.post("/v1/commit", json={"org_id": 2, "request_id": "req-2"}).status_code == 409


@pytest.mark.parametrize("body", [
    {"org_id": 2, "feature": "feature1", "units": 0, "request_id": "x"},
    {"org_id": 2, "feature": "feature1", "units": 5, "request_id": ""},
    {"org_id": 2, "feature": "feature1", "units": 5},
])
def test_invalid_requests_are_422(client, body):
    assert client.post("/v1/reserve", json=body).status_code == 422


def test_unknown_things_are_404(client):
    assert reserve(client, 1, feature="nope").status_code == 404
    assert reserve(client, 1, org=12345).status_code == 404
    assert client.post("/v1/commit", json={"org_id": 2, "request_id": "never"}).status_code == 404


def test_suspended_org_is_403(client):
    assert reserve(client, 1, org=99).status_code == 403
