from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_root_endpoint() -> None:
    response = client.get("/")

    assert response.status_code == 200
    payload = response.json()

    assert payload["message"] == "EYE is running"
    assert payload["environment"] == "development"
    assert payload["healthcheck"] == "/api/v1/health"


def test_health_endpoint() -> None:
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    payload = response.json()

    assert payload["status"] == "ok"
    assert payload["app_name"] == "EYE"
    assert payload["version"] == "0.1.0"
    assert payload["environment"] == "development"
