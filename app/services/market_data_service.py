from __future__ import annotations

import json
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import pandas as pd
import yfinance as yf

from app.core.config import DATA_DIR, Settings, get_settings
from app.schemas.market import MarketSymbol, MarketTimeframe


class MarketDataService:
    def __init__(
        self,
        cache_dir: Path | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.cache_dir = Path(cache_dir) if cache_dir is not None else DATA_DIR / "yf-cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._configure_yfinance_cache()

    def get_ohlcv(
        self,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
    ) -> pd.DataFrame:
        if self._should_use_massive(symbol):
            try:
                return self._get_ohlcv_from_massive(symbol, timeframe)
            except ValueError as exc:
                if self._should_fallback_to_fmp(
                    symbol=symbol,
                    timeframe=timeframe,
                    error_text=str(exc),
                ):
                    return self._get_ohlcv_from_fmp(symbol, timeframe)
                raise

        provider_symbol = self._map_symbol(symbol)
        provider_interval = self._map_interval(timeframe)
        provider_period = self._map_period(timeframe)

        try:
            raw_df = yf.download(
                provider_symbol,
                period=provider_period,
                interval=provider_interval,
                auto_adjust=False,
                progress=False,
                threads=False,
                prepost=False,
            )
        except Exception as exc:
            raise ValueError(
                f"Failed to download OHLCV for symbol '{self._display_value(symbol)}' "
                f"and timeframe '{self._display_value(timeframe)}': {exc}"
            ) from exc

        normalized_df = self._normalize_downloaded_frame(raw_df)

        if timeframe == MarketTimeframe.h4:
            normalized_df = self._resample_to_4h(normalized_df)

        if normalized_df.empty:
            raise ValueError(
                f"No OHLCV data returned for symbol '{self._display_value(symbol)}' "
                f"and timeframe '{self._display_value(timeframe)}'."
            )

        return normalized_df

    def _should_use_massive(self, symbol: MarketSymbol) -> bool:
        provider = str(self.settings.market_data_provider).strip().lower()

        return (
            provider == "massive"
            and bool(self.settings.market_data_realtime_enabled)
            and bool(self.settings.massive_api_key)
            and symbol in {MarketSymbol.ndx}
        )

    def _get_ohlcv_from_massive(
        self,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
    ) -> pd.DataFrame:
        provider_symbol = self._map_massive_symbol(symbol)
        multiplier, timespan = self._map_massive_range(timeframe)
        date_from, date_to = self._build_massive_date_range(timeframe)

        query = urlencode(
            {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
                "apiKey": self.settings.massive_api_key,
            }
        )
        url = (
            f"https://api.massive.com/v2/aggs/ticker/{provider_symbol}"
            f"/range/{multiplier}/{timespan}/{date_from}/{date_to}?{query}"
        )

        payload = self._fetch_json_via_curl(url)
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list) or not results:
            raise ValueError(
                f"No OHLCV data returned for symbol '{self._display_value(symbol)}' "
                f"and timeframe '{self._display_value(timeframe)}'."
            )

        rows: list[dict[str, object]] = []
        for item in results:
            if not isinstance(item, dict):
                continue

            timestamp_ms = item.get("t")
            open_ = item.get("o")
            high = item.get("h")
            low = item.get("l")
            close = item.get("c")
            volume = item.get("v", 0)

            if (
                timestamp_ms is None
                or open_ is None
                or high is None
                or low is None
                or close is None
            ):
                continue

            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(
                        float(timestamp_ms) / 1000.0,
                        tz=timezone.utc,
                    ),
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume or 0.0),
                }
            )

        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError(
                f"No OHLCV data returned for symbol '{self._display_value(symbol)}' "
                f"and timeframe '{self._display_value(timeframe)}'."
            )

        frame = frame.sort_values("timestamp").reset_index(drop=True)

        if timeframe == MarketTimeframe.h4:
            frame = self._resample_to_4h(frame)

        return frame

    def _should_fallback_to_fmp(
        self,
        *,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
        error_text: str,
    ) -> bool:
        return (
            symbol == MarketSymbol.spx
            and timeframe in {MarketTimeframe.h1, MarketTimeframe.h4, MarketTimeframe.d1}
            and bool(self.settings.fmp_api_key)
            and "NOT_AUTHORIZED" in str(error_text)
        )

    def _get_ohlcv_from_fmp(
        self,
        symbol: MarketSymbol,
        timeframe: MarketTimeframe,
    ) -> pd.DataFrame:
        provider_symbol = self._map_fmp_symbol(symbol)

        if timeframe == MarketTimeframe.h1:
            url = self._build_fmp_intraday_url(
                interval="1hour",
                symbol=provider_symbol,
            )
            payload = self._fetch_json_via_curl(url)
            records = self._extract_fmp_records(payload)
            frame = self._normalize_fmp_frame(records)
            if frame.empty:
                raise ValueError(
                    f"No OHLCV data returned from FMP for symbol '{self._display_value(symbol)}' "
                    f"and timeframe '{self._display_value(timeframe)}'."
                )
            return frame

        if timeframe == MarketTimeframe.h4:
            url = self._build_fmp_intraday_url(
                interval="1hour",
                symbol=provider_symbol,
            )
            payload = self._fetch_json_via_curl(url)
            records = self._extract_fmp_records(payload)
            frame = self._normalize_fmp_frame(records)
            if frame.empty:
                raise ValueError(
                    f"No OHLCV data returned from FMP for symbol '{self._display_value(symbol)}' "
                    f"and timeframe '{self._display_value(timeframe)}'."
                )
            return self._resample_to_4h(frame)

        if timeframe == MarketTimeframe.d1:
            url = self._build_fmp_eod_url(symbol=provider_symbol)
            payload = self._fetch_json_via_curl(url)
            records = self._extract_fmp_records(payload)
            frame = self._normalize_fmp_frame(records)
            if frame.empty:
                raise ValueError(
                    f"No OHLCV data returned from FMP for symbol '{self._display_value(symbol)}' "
                    f"and timeframe '{self._display_value(timeframe)}'."
                )
            return frame

        raise ValueError(
            f"Unsupported FMP timeframe '{self._display_value(timeframe)}'."
        )

    def _map_fmp_symbol(self, symbol: MarketSymbol) -> str:
        symbol_map = {
            MarketSymbol.spx: "^GSPC",
        }

        try:
            return symbol_map[symbol]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported FMP symbol '{self._display_value(symbol)}'."
            ) from exc

    def _build_fmp_intraday_url(self, *, interval: str, symbol: str) -> str:
        return (
            "https://financialmodelingprep.com/stable/"
            f"historical-chart/{interval}?symbol={symbol}&apikey={self.settings.fmp_api_key}"
        )

    def _build_fmp_eod_url(self, *, symbol: str) -> str:
        return (
            "https://financialmodelingprep.com/stable/"
            f"historical-price-eod/full?symbol={symbol}&apikey={self.settings.fmp_api_key}"
        )

    def _extract_fmp_records(self, payload) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            for key in ("historical", "data", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]

        return []

    def _normalize_fmp_frame(self, records: list[dict]) -> pd.DataFrame:
        rows: list[dict[str, object]] = []

        for item in records:
            timestamp_value = (
                item.get("date")
                or item.get("datetime")
                or item.get("timestamp")
            )
            open_value = item.get("open")
            high_value = item.get("high")
            low_value = item.get("low")
            close_value = item.get("close")
            volume_value = item.get("volume", 0)

            if (
                timestamp_value is None
                or open_value is None
                or high_value is None
                or low_value is None
                or close_value is None
            ):
                continue

            parsed_ts = pd.to_datetime(timestamp_value, utc=True, errors="coerce")
            if pd.isna(parsed_ts):
                continue

            rows.append(
                {
                    "timestamp": parsed_ts,
                    "open": float(open_value),
                    "high": float(high_value),
                    "low": float(low_value),
                    "close": float(close_value),
                    "volume": float(volume_value or 0.0),
                }
            )

        if not rows:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        frame = pd.DataFrame(rows)
        frame = frame.sort_values("timestamp").reset_index(drop=True)
        return frame

    def _configure_yfinance_cache(self) -> None:
        try:
            if hasattr(yf, "set_tz_cache_location"):
                yf.set_tz_cache_location(str(self.cache_dir))
        except Exception:
            pass

    def _display_value(self, value) -> str:
        return str(getattr(value, "value", value))

    def _map_symbol(self, symbol: MarketSymbol) -> str:
        # Free-mode fallback:
        # SPX uses SPY ETF as a practical proxy to avoid paid index data entitlements.
        # Signal structure stays useful, but absolute price levels are SPY ETF prices, not raw SPX index points.
        symbol_map = {
            MarketSymbol.ndx: "^NDX",
            MarketSymbol.spx: "SPY",
            MarketSymbol.btc: "BTC-USD",
            MarketSymbol.wti: "CL=F",
        }

        try:
            return symbol_map[symbol]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported symbol '{self._display_value(symbol)}'."
            ) from exc

    def _map_massive_symbol(self, symbol: MarketSymbol) -> str:
        symbol_map = {
            MarketSymbol.ndx: "I:NDX",
            MarketSymbol.spx: "I:SPX",
        }

        try:
            return symbol_map[symbol]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported symbol '{self._display_value(symbol)}' for Massive OHLCV."
            ) from exc

    def _map_interval(self, timeframe: MarketTimeframe) -> str:
        interval_map = {
            MarketTimeframe.m1: "1m",
            MarketTimeframe.h1: "60m",
            MarketTimeframe.h4: "60m",
            MarketTimeframe.d1: "1d",
        }

        try:
            return interval_map[timeframe]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported timeframe '{self._display_value(timeframe)}'."
            ) from exc

    def _map_period(self, timeframe: MarketTimeframe) -> str:
        period_map = {
            MarketTimeframe.m1: "5d",
            MarketTimeframe.h1: "60d",
            MarketTimeframe.h4: "60d",
            MarketTimeframe.d1: "2y",
        }

        try:
            return period_map[timeframe]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported timeframe '{self._display_value(timeframe)}'."
            ) from exc

    def _map_massive_range(self, timeframe: MarketTimeframe) -> tuple[int, str]:
        range_map = {
            MarketTimeframe.h1: (1, "hour"),
            MarketTimeframe.h4: (1, "hour"),
            MarketTimeframe.d1: (1, "day"),
        }

        try:
            return range_map[timeframe]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported Massive timeframe '{self._display_value(timeframe)}'."
            ) from exc

    def _build_massive_date_range(
        self,
        timeframe: MarketTimeframe,
    ) -> tuple[str, str]:
        today: date = datetime.now(timezone.utc).date()

        lookback_days = {
            MarketTimeframe.h1: 14,
            MarketTimeframe.h4: 90,
            MarketTimeframe.d1: 730,
        }

        try:
            start_date = today - timedelta(days=lookback_days[timeframe])
        except KeyError as exc:
            raise ValueError(
                f"Unsupported Massive timeframe '{self._display_value(timeframe)}'."
            ) from exc

        return start_date.isoformat(), today.isoformat()

    def _normalize_downloaded_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return self._empty_ohlcv_frame()

        normalized = frame.copy()

        if isinstance(normalized.columns, pd.MultiIndex):
            normalized.columns = [str(column[0]).strip().lower() for column in normalized.columns]
        else:
            normalized.columns = [str(column).strip().lower() for column in normalized.columns]

        normalized = normalized.reset_index()

        first_column = str(normalized.columns[0]).strip().lower()
        if first_column not in {"datetime", "date", "timestamp"}:
            normalized = normalized.rename(columns={normalized.columns[0]: "timestamp"})
        else:
            normalized = normalized.rename(columns={normalized.columns[0]: "timestamp"})

        rename_map = {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "adj close": "close",
            "volume": "volume",
        }
        normalized = normalized.rename(columns=rename_map)
        normalized = normalized.loc[:, ~normalized.columns.duplicated()]

        required_columns = ["timestamp", "open", "high", "low", "close", "volume"]
        for column in required_columns:
            if column not in normalized.columns:
                normalized[column] = pd.NA

        normalized["timestamp"] = pd.to_datetime(
            normalized["timestamp"],
            errors="coerce",
            utc=True,
        )

        numeric_columns = ["open", "high", "low", "close", "volume"]
        for column in numeric_columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        normalized = normalized[required_columns].dropna(subset=["timestamp", "open", "high", "low", "close"])
        normalized = normalized.sort_values("timestamp").reset_index(drop=True)

        return normalized

    def _normalize_massive_aggregate_payload(self, payload: dict) -> pd.DataFrame:
        if not isinstance(payload, dict):
            return self._empty_ohlcv_frame()

        raw_results = payload.get("results")
        if not isinstance(raw_results, list) or not raw_results:
            return self._empty_ohlcv_frame()

        normalized = pd.DataFrame(raw_results).copy()
        rename_map = {
            "t": "timestamp",
            "timestamp": "timestamp",
            "o": "open",
            "open": "open",
            "h": "high",
            "high": "high",
            "l": "low",
            "low": "low",
            "c": "close",
            "close": "close",
            "v": "volume",
            "volume": "volume",
        }
        normalized = normalized.rename(columns=rename_map)

        required_columns = ["timestamp", "open", "high", "low", "close", "volume"]
        for column in required_columns:
            if column not in normalized.columns:
                normalized[column] = pd.NA

        normalized["timestamp"] = pd.to_datetime(
            normalized["timestamp"],
            errors="coerce",
            utc=True,
            unit="ms",
        )

        for column in ["open", "high", "low", "close", "volume"]:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        normalized = normalized[required_columns].dropna(
            subset=["timestamp", "open", "high", "low", "close"]
        )
        return normalized.sort_values("timestamp").reset_index(drop=True)

    def _resample_to_4h(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame

        working = frame.copy()
        working = working.set_index("timestamp")

        resampled = (
            working.resample("4h", label="right", closed="right")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )

        return resampled

    def _empty_ohlcv_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )

    def _fetch_json_via_curl(self, url: str) -> dict:
        result = subprocess.run(
            [
                "curl.exe",
                "-sS",
                "-L",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if result.returncode != 0:
            lowered_stderr = stderr.lower()
            if "schannel" in lowered_stderr or "sec_e_no_credentials" in lowered_stderr:
                return self._fetch_json_via_urllib(url)
            raise ValueError(
                f"curl.exe failed with exit code {result.returncode}: {stderr or stdout}"
            )

        if not stdout:
            raise ValueError("curl.exe returned an empty response from Massive.")

        return self._parse_massive_payload(stdout)

    def _fetch_json_via_urllib(self, url: str) -> dict:
        request = Request(
            url,
            headers={
                "User-Agent": "EYE/1.0",
            },
        )

        try:
            with urlopen(request, timeout=20) as response:
                stdout = response.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return self._parse_massive_payload(body)

        if not stdout or not stdout.strip():
            raise ValueError("urllib returned an empty response from Massive.")

        return self._parse_massive_payload(stdout)

    def _parse_massive_payload(self, raw_text: str) -> dict:
        try:
            payload = json.loads(raw_text)
        except Exception as exc:
            raise ValueError(
                f"Massive response parsing failed: {exc}. Raw response: {raw_text[:400]}"
            ) from exc

        if isinstance(payload, dict) and payload.get("status") in {"ERROR", "NOT_AUTHORIZED"}:
            raise ValueError(f"Massive returned error payload: {payload}")

        return payload
