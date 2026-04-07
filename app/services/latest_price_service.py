from __future__ import annotations

import json
import subprocess
from urllib.parse import urlencode

import yfinance as yf

from app.core.config import Settings, get_settings
from app.schemas.market import MarketSymbol


DISPLAY_NAMES = {
    MarketSymbol.ndx: "Nasdaq 100 (NDX)",
    MarketSymbol.spx: "S&P 500 (SPX)",
    MarketSymbol.btc: "Bitcoin (BTC)",
}

MASSIVE_INDEX_TICKERS = {
    MarketSymbol.ndx: "I:NDX",
    MarketSymbol.spx: "I:SPX",
}

YF_LIVE_TICKERS = {
    MarketSymbol.btc: "BTC-USD",
}


class LatestPriceService:
    def __init__(
        self,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()

    def get_latest_price(self, symbol: MarketSymbol) -> dict:
        if symbol == MarketSymbol.btc:
            return self._get_latest_price_from_yfinance(symbol)

        provider = str(self.settings.market_data_provider).strip().lower()

        if provider == "massive":
            return self._get_latest_price_from_massive(symbol)

        raise ValueError(
            "Latest price provider not available for this symbol. "
            "BTC uses yfinance live-ish price, while NDX/SPX currently require Massive."
        )

    def _get_latest_price_from_yfinance(self, symbol: MarketSymbol) -> dict:
        provider_symbol = YF_LIVE_TICKERS.get(symbol)
        if provider_symbol is None:
            raise ValueError(f"Unsupported symbol '{symbol.value}' for yfinance latest price.")

        ticker = yf.Ticker(provider_symbol)

        fast_info = {}
        try:
            fast_info = dict(ticker.fast_info or {})
        except Exception:
            fast_info = {}

        price = self._first_number(
            [
                fast_info.get("lastPrice"),
                fast_info.get("last_price"),
                fast_info.get("regularMarketPrice"),
                fast_info.get("regular_market_price"),
            ]
        )

        previous_close = self._first_number(
            [
                fast_info.get("previousClose"),
                fast_info.get("previous_close"),
                fast_info.get("regularMarketPreviousClose"),
                fast_info.get("regular_market_previous_close"),
            ]
        )

        day_high = self._first_number(
            [
                fast_info.get("dayHigh"),
                fast_info.get("day_high"),
            ]
        )

        day_low = self._first_number(
            [
                fast_info.get("dayLow"),
                fast_info.get("day_low"),
            ]
        )

        if price is None or previous_close is None:
            history = ticker.history(period="1d", interval="1m", auto_adjust=False)
            if history is None or history.empty:
                raise ValueError(
                    f"No yfinance latest price data returned for symbol '{symbol.value}'."
                )

            history = history.reset_index()
            history.columns = [str(column).strip() for column in history.columns]

            time_column = history.columns[0]
            last_row = history.iloc[-1]
            prev_row = history.iloc[-2] if len(history) > 1 else last_row

            price = self._first_number([last_row.get("Close")])
            previous_close = self._first_number([prev_row.get("Close")])

            day_high = self._first_number([history["High"].max()]) if "High" in history.columns else None
            day_low = self._first_number([history["Low"].min()]) if "Low" in history.columns else None
            as_of_utc = str(last_row.get(time_column))
        else:
            as_of_utc = None

        if price is None:
            raise ValueError(
                f"yfinance did not return a usable latest price for symbol '{symbol.value}'."
            )

        if previous_close is None:
            previous_close = price

        abs_change = price - previous_close
        pct_change = 0.0 if previous_close == 0 else (abs_change / previous_close) * 100.0

        return {
            "asset": DISPLAY_NAMES.get(symbol, symbol.value),
            "symbol": symbol.value,
            "provider_symbol": provider_symbol,
            "price": round(price, 4),
            "abs_change": round(abs_change, 4),
            "pct_change": round(pct_change, 4),
            "day_high": round(day_high, 4) if day_high is not None else None,
            "day_low": round(day_low, 4) if day_low is not None else None,
            "as_of_utc": as_of_utc,
            "market_status": "crypto_live",
            "is_realtime": True,
            "provider_requested": "yfinance",
            "provider_effective": "yfinance_fast_info_btc",
        }

    def _get_latest_price_from_massive(self, symbol: MarketSymbol) -> dict:
        if not self.settings.market_data_realtime_enabled:
            raise ValueError(
                "Realtime market data is disabled. "
                "Set EYE_MARKET_DATA_REALTIME_ENABLED=true."
            )

        if not self.settings.massive_api_key:
            raise ValueError(
                "Massive API key missing. "
                "Set EYE_MASSIVE_API_KEY in your .env file."
            )

        provider_symbol = MASSIVE_INDEX_TICKERS.get(symbol)
        if provider_symbol is None:
            raise ValueError(f"Unsupported symbol '{symbol.value}' for Massive.")

        query = urlencode(
            {
                "ticker": provider_symbol,
                "apiKey": self.settings.massive_api_key,
            }
        )
        url = f"https://api.massive.com/v3/snapshot?{query}"

        payload = self._fetch_json_via_curl(url)
        result = self._extract_massive_result(payload)
        if not isinstance(result, dict):
            raise ValueError("Massive snapshot returned no usable result.")

        session = result.get("session") or {}
        if not isinstance(session, dict):
            session = {}

        price = self._first_number(
            [
                result.get("value"),
                session.get("value"),
                self._nested_get(result, "last_trade", "price"),
                self._nested_get(result, "last_quote", "midpoint"),
                session.get("close"),
                result.get("close"),
            ]
        )

        abs_change = self._first_number(
            [
                session.get("change"),
                result.get("change"),
            ]
        )

        pct_change = self._first_number(
            [
                session.get("change_percent"),
                result.get("change_percent"),
            ]
        )

        as_of_utc = self._first_text(
            [
                result.get("updated"),
                result.get("last_updated"),
                self._nested_get(result, "last_trade", "sip_timestamp"),
                self._nested_get(result, "last_trade", "timestamp"),
                session.get("updated"),
            ]
        )

        market_status = self._first_text(
            [
                result.get("market_status"),
                result.get("session_name"),
                "unknown",
            ]
        )

        if price is None:
            raise ValueError(
                f"Massive snapshot did not contain a usable price for symbol '{symbol.value}'."
            )

        if abs_change is None:
            abs_change = 0.0

        if pct_change is None:
            pct_change = 0.0

        return {
            "asset": DISPLAY_NAMES.get(symbol, symbol.value),
            "symbol": symbol.value,
            "provider_symbol": provider_symbol,
            "price": round(price, 4),
            "abs_change": round(abs_change, 4),
            "pct_change": round(pct_change, 4),
            "as_of_utc": as_of_utc,
            "market_status": market_status,
            "is_realtime": True,
            "provider_requested": self.settings.market_data_provider,
            "provider_effective": "massive_unified_snapshot_via_curl",
        }

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
            raise ValueError(
                f"curl.exe failed with exit code {result.returncode}: {stderr or stdout}"
            )

        if not stdout:
            raise ValueError("curl.exe returned an empty response from Massive.")

        try:
            payload = json.loads(stdout)
        except Exception as exc:
            raise ValueError(
                f"Massive response parsing failed: {exc}. Raw response: {stdout[:400]}"
            ) from exc

        if isinstance(payload, dict) and payload.get("status") == "ERROR":
            raise ValueError(f"Massive returned error payload: {payload}")

        return payload

    def _extract_massive_result(self, payload: dict) -> dict | None:
        if not isinstance(payload, dict):
            return None

        results = payload.get("results")
        if isinstance(results, list) and results:
            first = results[0]
            if isinstance(first, dict):
                return first

        if isinstance(results, dict):
            return results

        return payload if payload else None

    def _nested_get(self, data: dict, *keys: str):
        current = data
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    def _first_number(self, candidates: list[object]) -> float | None:
        for candidate in candidates:
            try:
                if candidate is None or candidate == "":
                    continue
                return float(candidate)
            except (TypeError, ValueError):
                continue
        return None

    def _first_text(self, candidates: list[object]) -> str | None:
        for candidate in candidates:
            if candidate is None:
                continue
            text = str(candidate).strip()
            if text:
                return text
        return None
