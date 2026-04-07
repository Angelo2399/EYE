from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_run_asset_updates_route_calls_runner_and_returns_counts(monkeypatch) -> None:
    from app.api import routes_asset_updates

    observed: dict[str, object] = {}

    def fake_run_asset_updates(*, assets=None, timeframe=None, timezone_name=None):
        observed["assets"] = [
            asset.value if hasattr(asset, "value") else asset
            for asset in list(assets or [])
        ]
        observed["timeframe"] = (
            timeframe.value if hasattr(timeframe, "value") else timeframe
        )
        observed["timezone_name"] = timezone_name
        return {
            "results": [
                {
                    "asset": "NDX",
                    "timeframe": "1h",
                    "response": {
                        "asset": "Nasdaq 100",
                        "action": "long",
                    },
                }
            ],
            "errors": [
                {
                    "asset": "WTI",
                    "timeframe": "1h",
                    "error": "WTI feed unavailable.",
                }
            ],
        }

    monkeypatch.setattr(
        routes_asset_updates.asset_update_runner_service,
        "run_asset_updates",
        fake_run_asset_updates,
    )

    client = TestClient(app)

    response = client.post(
        "/api/v1/asset-updates/run",
        json={
            "assets": ["NDX", "WTI"],
            "timeframe": "1h",
        },
        headers={"X-EYE-Timezone": "Europe/London"},
    )

    assert response.status_code == 200
    payload = response.json()

    assert observed == {
        "assets": ["NDX", "WTI"],
        "timeframe": "1h",
        "timezone_name": "Europe/London",
    }
    assert payload["processed_count"] == 2
    assert payload["error_count"] == 1
    assert payload["results"] == [
        {
            "asset": "NDX",
            "timeframe": "1h",
            "response": {
                "asset": "Nasdaq 100",
                "action": "long",
            },
        }
    ]
    assert payload["errors"] == [
        {
            "asset": "WTI",
            "timeframe": "1h",
            "error": "WTI feed unavailable.",
        }
    ]


def test_run_asset_updates_route_uses_default_payload_values(monkeypatch) -> None:
    from app.api import routes_asset_updates

    observed: dict[str, object] = {}

    def fake_run_asset_updates(*, assets=None, timeframe=None, timezone_name=None):
        observed["assets"] = assets
        observed["timeframe"] = (
            timeframe.value if hasattr(timeframe, "value") else timeframe
        )
        observed["timezone_name"] = timezone_name
        return {
            "results": [],
            "errors": [],
        }

    monkeypatch.setattr(
        routes_asset_updates.asset_update_runner_service,
        "run_asset_updates",
        fake_run_asset_updates,
    )

    client = TestClient(app)

    response = client.post(
        "/api/v1/asset-updates/run",
        json={},
    )

    assert response.status_code == 200
    payload = response.json()

    assert observed == {
        "assets": None,
        "timeframe": "1h",
        "timezone_name": None,
    }
    assert payload == {
        "results": [],
        "errors": [],
        "processed_count": 0,
        "error_count": 0,
    }
