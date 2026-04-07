import pandas as pd
import pytest
from types import SimpleNamespace

from app.services.market_data_service import MarketDataService


def test_get_ohlcv_returns_standardized_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    raw_frame = pd.DataFrame(
        {
            "Date": pd.to_datetime(
                ["2026-03-24 00:00:00+00:00", "2026-03-25 00:00:00+00:00"],
                utc=True,
            ),
            "Open": [24000.0, 24100.0],
            "High": [24200.0, 24300.0],
            "Low": [23900.0, 24050.0],
            "Close": [24150.0, 24250.0],
            "Volume": [1000, 1200],
        }
    )

    def fake_download(*args, **kwargs) -> pd.DataFrame:
        return raw_frame

    monkeypatch.setattr("app.services.market_data_service.yf.download", fake_download)

    service = MarketDataService(
        settings=SimpleNamespace(
            market_data_provider="yfinance",
            market_data_realtime_enabled=False,
            massive_api_key=None,
        )
    )
    result = service.get_ohlcv("NDX", "1d")

    assert list(result.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(result) == 2
    assert result.iloc[0]["open"] == 24000.0
    assert result.iloc[1]["close"] == 24250.0
    assert str(result["timestamp"].dtype).startswith("datetime64[ns, UTC]")


def test_get_ohlcv_resamples_4h(monkeypatch: pytest.MonkeyPatch) -> None:
    raw_frame = pd.DataFrame(
        {
            "Datetime": pd.to_datetime(
                [
                    "2026-03-25 09:00:00+00:00",
                    "2026-03-25 10:00:00+00:00",
                    "2026-03-25 11:00:00+00:00",
                    "2026-03-25 12:00:00+00:00",
                ],
                utc=True,
            ),
            "Open": [100.0, 101.0, 102.0, 103.0],
            "High": [101.0, 102.0, 103.0, 104.0],
            "Low": [99.0, 100.0, 101.0, 102.0],
            "Close": [100.5, 101.5, 102.5, 103.5],
            "Volume": [10, 20, 30, 40],
        }
    )

    def fake_download(*args, **kwargs) -> pd.DataFrame:
        return raw_frame

    monkeypatch.setattr("app.services.market_data_service.yf.download", fake_download)

    service = MarketDataService(
        settings=SimpleNamespace(
            market_data_provider="yfinance",
            market_data_realtime_enabled=False,
            massive_api_key=None,
        )
    )
    result = service.get_ohlcv("SPX", "4h")

    assert list(result.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(result) >= 1
    assert result.iloc[0]["volume"] > 0


def test_get_ohlcv_rejects_unsupported_symbol() -> None:
    service = MarketDataService(
        settings=SimpleNamespace(
            market_data_provider="yfinance",
            market_data_realtime_enabled=False,
            massive_api_key=None,
        )
    )

    with pytest.raises(ValueError, match="Unsupported symbol"):
        service.get_ohlcv("DJI", "1d")


def test_get_ohlcv_rejects_unsupported_timeframe() -> None:
    service = MarketDataService(
        settings=SimpleNamespace(
            market_data_provider="yfinance",
            market_data_realtime_enabled=False,
            massive_api_key=None,
        )
    )

    with pytest.raises(ValueError, match="Unsupported timeframe"):
        service.get_ohlcv("NDX", "15m")


def test_get_ohlcv_uses_massive_for_supported_indices(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch(self, url: str) -> dict:
        captured["url"] = url
        return {
            "results": [
                {
                    "t": 1774569600000,
                    "o": 24000.0,
                    "h": 24200.0,
                    "l": 23900.0,
                    "c": 24150.0,
                    "v": 1000,
                },
                {
                    "t": 1774656000000,
                    "o": 24100.0,
                    "h": 24300.0,
                    "l": 24050.0,
                    "c": 24250.0,
                    "v": 1200,
                },
            ]
        }

    def forbidden_download(*args, **kwargs):
        raise AssertionError("yfinance must not be used for Massive-backed NDX/SPX.")

    monkeypatch.setattr(
        MarketDataService,
        "_fetch_json_via_curl",
        fake_fetch,
    )
    monkeypatch.setattr("app.services.market_data_service.yf.download", forbidden_download)

    service = MarketDataService(
        settings=SimpleNamespace(
            market_data_provider="massive",
            market_data_realtime_enabled=True,
            massive_api_key="test-key",
        )
    )
    result = service.get_ohlcv("NDX", "1d")

    assert "api.massive.com/v2/aggs/ticker/I:NDX/range/1/day/" in str(captured["url"])
    assert "apiKey=test-key" in str(captured["url"])
    assert list(result.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(result) == 2
    assert result.iloc[0]["open"] == 24000.0
    assert result.iloc[1]["close"] == 24250.0
    assert str(result["timestamp"].dtype).startswith("datetime64[ns, UTC]")
