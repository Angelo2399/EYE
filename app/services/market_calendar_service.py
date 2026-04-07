from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta, timezone
from html import unescape
from io import StringIO
from pathlib import Path
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd

from app.core.config import DATA_DIR
from app.schemas.market import MarketSymbol


class MarketCalendarService:
    US_EQUITIES_TZ = ZoneInfo("America/New_York")
    WTI_TZ = ZoneInfo("America/New_York")

    US_EQUITIES_OPEN = time(9, 30)
    US_EQUITIES_CLOSE = time(16, 0)

    WTI_OPEN_SUNDAY = time(18, 0)
    WTI_DAILY_BREAK_START = time(17, 0)
    WTI_DAILY_BREAK_END = time(18, 0)
    WTI_FRIDAY_CLOSE = time(17, 0)

    US_EQUITIES_SOURCES = (
        (
            "Nasdaq official holiday schedule",
            "https://www.nasdaq.com/market-activity/stock-market-holiday-schedule",
        ),
        (
            "NYSE official holiday schedule",
            "https://www.nyse.com/markets/hours-calendars",
        ),
    )

    EURONEXT_SOURCE = (
        "Euronext official trading hours and holidays",
        "https://www.euronext.com/en/trading/trading-hours-holidays",
    )

    WTI_SOURCE = (
        "CME/NYMEX official holiday hours",
        "https://www.cmegroup.com/trading-hours.html",
    )

    CACHE_TTL_HOURS = {
        "us_equities_cash": 24,
        "euronext_cash": 24,
        "nymex_wti": 6,
    }

    US_EQUITIES_2026_HOLIDAYS = {
        date(2026, 1, 1): "New Year's Day",
        date(2026, 1, 19): "Martin Luther King, Jr. Day",
        date(2026, 2, 16): "Presidents Day",
        date(2026, 4, 3): "Good Friday",
        date(2026, 5, 25): "Memorial Day",
        date(2026, 6, 19): "Juneteenth",
        date(2026, 7, 3): "Independence Day (Observed)",
        date(2026, 9, 7): "Labor Day",
        date(2026, 11, 26): "Thanksgiving Day",
        date(2026, 12, 25): "Christmas Day",
    }

    US_EQUITIES_2026_EARLY_CLOSES = {
        date(2026, 11, 27): {
            "name": "Day after Thanksgiving",
            "close_time": time(13, 0),
        },
        date(2026, 12, 24): {
            "name": "Christmas Eve",
            "close_time": time(13, 0),
        },
    }

    STATIC_US_EQUITIES_2026 = {
        "holidays": {
            "2026-01-01": "New Year's Day",
            "2026-01-19": "Martin Luther King, Jr. Day",
            "2026-02-16": "Presidents Day",
            "2026-04-03": "Good Friday",
            "2026-05-25": "Memorial Day",
            "2026-06-19": "Juneteenth",
            "2026-07-03": "Independence Day (Observed)",
            "2026-09-07": "Labor Day",
            "2026-11-26": "Thanksgiving Day",
            "2026-12-25": "Christmas Day",
        },
        "early_closes": {
            "2026-11-27": {
                "name": "Day after Thanksgiving",
                "close_time": "13:00",
            },
            "2026-12-24": {
                "name": "Christmas Eve",
                "close_time": "13:00",
            },
        },
        "source": "Static fallback aligned to official 2026 NYSE/Nasdaq holiday calendars",
    }

    STATIC_WTI_SCHEDULE = {
        "special_sessions": {},
        "source": "CME/NYMEX regular session logic currently active",
    }

    def __init__(
        self,
        *,
        cache_dir: Path | None = None,
        fetcher=None,
        now_provider=None,
    ) -> None:
        self.cache_dir = (
            Path(cache_dir) if cache_dir is not None else DATA_DIR / "market-calendars"
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.fetcher = fetcher or self._default_fetcher
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def warm_reference_calendars(
        self,
        *,
        years: list[int] | None = None,
    ) -> dict[str, object]:
        resolved_years = years or [self.now_provider().year, self.now_provider().year + 1]
        warmed: dict[str, dict[int, object | None]] = {
            "us_equities_cash": {},
            "euronext_cash": {},
            "nymex_wti": {},
        }

        for year in resolved_years:
            warmed["us_equities_cash"][year] = self._load_us_equities_schedule(year)
            try:
                warmed["euronext_cash"][year] = self._load_euronext_schedule(year)
            except Exception:
                warmed["euronext_cash"][year] = None
            try:
                warmed["nymex_wti"][year] = self._load_wti_schedule(year)
            except Exception:
                warmed["nymex_wti"][year] = None

        return warmed

    def get_market_status(
        self,
        *,
        asset: MarketSymbol | str,
        current_dt: datetime | None = None,
        timezone_name: str = "Europe/Madrid",
    ) -> dict[str, object]:
        normalized_asset = self._normalize_asset(asset)
        current_dt_utc = self._normalize_current_dt(current_dt)

        if normalized_asset in {"ndx", "spx"}:
            return self._get_us_equities_status(
                asset=normalized_asset,
                current_dt_utc=current_dt_utc,
                timezone_name=timezone_name,
            )

        if normalized_asset == "wti":
            return self._get_wti_status(
                asset=normalized_asset,
                current_dt_utc=current_dt_utc,
                timezone_name=timezone_name,
            )

        raise ValueError(f"Unsupported asset '{normalized_asset}' for market calendar.")

    def _get_us_equities_status(
        self,
        *,
        asset: str,
        current_dt_utc: datetime,
        timezone_name: str,
    ) -> dict[str, object]:
        venue_dt = current_dt_utc.astimezone(self.US_EQUITIES_TZ)
        local_dt = current_dt_utc.astimezone(ZoneInfo(timezone_name))
        venue_date = venue_dt.date()
        venue_time = venue_dt.time()
        schedule = self._load_us_equities_schedule(venue_date.year)
        holiday_name = schedule["holidays"].get(venue_date.isoformat())
        if holiday_name is not None:
            return self._build_result(
                asset=asset,
                venue="us_equities_cash",
                market_status="closed_holiday",
                is_open_now=False,
                is_tradable_now=False,
                holiday_name=holiday_name,
                official_reason=f"U.S. cash equities closed for {holiday_name}.",
                next_open_local=self._next_us_equities_open_local(
                    from_venue_dt=venue_dt,
                    timezone_name=timezone_name,
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        if venue_dt.weekday() >= 5:
            return self._build_result(
                asset=asset,
                venue="us_equities_cash",
                market_status="closed_weekend",
                is_open_now=False,
                is_tradable_now=False,
                holiday_name=None,
                official_reason="U.S. cash equities closed for the weekend.",
                next_open_local=self._next_us_equities_open_local(
                    from_venue_dt=venue_dt,
                    timezone_name=timezone_name,
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        early_close = schedule["early_closes"].get(venue_date.isoformat())
        session_close = (
            self._parse_hhmm(str(early_close["close_time"]))
            if early_close is not None
            else self.US_EQUITIES_CLOSE
        )

        is_open_now = self.US_EQUITIES_OPEN <= venue_time < session_close
        is_tradable_now = is_open_now

        if early_close is not None:
            early_close_name = str(early_close["name"])

            if early_close_name.strip().lower() == "early close":
                static_2026 = self.STATIC_US_EQUITIES_2026.get("early_closes", {})
                static_match = static_2026.get(venue_date.isoformat())
                if isinstance(static_match, dict) and static_match.get("name"):
                    early_close_name = str(static_match["name"])

            return self._build_result(
                asset=asset,
                venue="us_equities_cash",
                market_status="early_close",
                is_open_now=is_open_now,
                is_tradable_now=is_tradable_now,
                holiday_name=early_close_name,
                official_reason=(
                    f"U.S. cash equities early close for {early_close_name}."
                ),
                next_open_local=(
                    None
                    if is_open_now
                    else self._next_us_equities_open_local(
                        from_venue_dt=venue_dt,
                        timezone_name=timezone_name,
                    )
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        if is_open_now:
            return self._build_result(
                asset=asset,
                venue="us_equities_cash",
                market_status="open",
                is_open_now=True,
                is_tradable_now=True,
                holiday_name=None,
                official_reason="Regular U.S. cash equities session.",
                next_open_local=None,
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        return self._build_result(
            asset=asset,
            venue="us_equities_cash",
            market_status="closed_session",
            is_open_now=False,
            is_tradable_now=False,
            holiday_name=None,
            official_reason="U.S. cash equities outside regular session hours.",
            next_open_local=self._next_us_equities_open_local(
                from_venue_dt=venue_dt,
                timezone_name=timezone_name,
            ),
            source=str(schedule["source"]),
            venue_dt=venue_dt,
            local_dt=local_dt,
        )

    def _get_wti_status(
        self,
        *,
        asset: str,
        current_dt_utc: datetime,
        timezone_name: str,
    ) -> dict[str, object]:
        venue_dt = current_dt_utc.astimezone(self.WTI_TZ)
        local_dt = current_dt_utc.astimezone(ZoneInfo(timezone_name))
        venue_date = venue_dt.date()
        weekday = venue_dt.weekday()
        venue_time = venue_dt.time()
        schedule = self._load_wti_schedule(venue_date.year)
        special_session = schedule["special_sessions"].get(venue_date.isoformat())

        if special_session is not None:
            special_status = str(special_session.get("market_status", "")).strip().lower()
            holiday_name = str(special_session.get("name") or "").strip() or None
            session_close = self._parse_flexible_time(special_session.get("close_time"))
            next_open_local = self._resolve_wti_special_next_open_local(
                special_session=special_session,
                from_venue_dt=venue_dt,
                timezone_name=timezone_name,
            )

            if special_status == "closed_holiday":
                return self._build_result(
                    asset=asset,
                    venue="nymex_wti",
                    market_status="closed_holiday",
                    is_open_now=False,
                    is_tradable_now=False,
                    holiday_name=holiday_name,
                    official_reason=f"NYMEX WTI closed for {holiday_name or 'holiday session'}.",
                    next_open_local=next_open_local,
                    source=str(schedule["source"]),
                    venue_dt=venue_dt,
                    local_dt=local_dt,
                )

            if special_status in {"reduced_hours", "early_close"}:
                is_open_now = (
                    session_close is not None
                    and venue_time < session_close
                    and not (
                        self.WTI_DAILY_BREAK_START
                        <= venue_time
                        < self.WTI_DAILY_BREAK_END
                    )
                )
                return self._build_result(
                    asset=asset,
                    venue="nymex_wti",
                    market_status=special_status,
                    is_open_now=is_open_now,
                    is_tradable_now=is_open_now,
                    holiday_name=holiday_name,
                    official_reason=(
                        f"NYMEX WTI {special_status.replace('_', ' ')} for {holiday_name or 'special session'}."
                    ),
                    next_open_local=(None if is_open_now else next_open_local),
                    source=str(schedule["source"]),
                    venue_dt=venue_dt,
                    local_dt=local_dt,
                )

        if weekday == 5:
            return self._build_result(
                asset=asset,
                venue="nymex_wti",
                market_status="closed_weekend",
                is_open_now=False,
                is_tradable_now=False,
                holiday_name=None,
                official_reason="NYMEX WTI electronic session closed for the weekend.",
                next_open_local=self._next_wti_open_local(
                    from_venue_dt=venue_dt,
                    timezone_name=timezone_name,
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        if weekday == 6:
            is_open_now = venue_time >= self.WTI_OPEN_SUNDAY
            return self._build_result(
                asset=asset,
                venue="nymex_wti",
                market_status="open" if is_open_now else "closed_session",
                is_open_now=is_open_now,
                is_tradable_now=is_open_now,
                holiday_name=None,
                official_reason=(
                    "NYMEX WTI electronic session opens Sunday at 18:00 ET."
                ),
                next_open_local=(
                    None
                    if is_open_now
                    else venue_dt.replace(
                        hour=18,
                        minute=0,
                        second=0,
                        microsecond=0,
                    ).astimezone(ZoneInfo(timezone_name)).isoformat()
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        if weekday == 4 and venue_time >= self.WTI_FRIDAY_CLOSE:
            return self._build_result(
                asset=asset,
                venue="nymex_wti",
                market_status="closed_session",
                is_open_now=False,
                is_tradable_now=False,
                holiday_name=None,
                official_reason=(
                    "NYMEX WTI electronic session closed after Friday 17:00 ET."
                ),
                next_open_local=self._next_wti_open_local(
                    from_venue_dt=venue_dt,
                    timezone_name=timezone_name,
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        in_daily_break = (
            self.WTI_DAILY_BREAK_START <= venue_time < self.WTI_DAILY_BREAK_END
        )
        if in_daily_break:
            return self._build_result(
                asset=asset,
                venue="nymex_wti",
                market_status="reduced_hours",
                is_open_now=False,
                is_tradable_now=False,
                holiday_name=None,
                official_reason=(
                    "NYMEX WTI daily maintenance break 17:00-18:00 ET."
                ),
                next_open_local=self._next_wti_open_local(
                    from_venue_dt=venue_dt,
                    timezone_name=timezone_name,
                ),
                source=str(schedule["source"]),
                venue_dt=venue_dt,
                local_dt=local_dt,
            )

        return self._build_result(
            asset=asset,
            venue="nymex_wti",
            market_status="open",
            is_open_now=True,
            is_tradable_now=True,
            holiday_name=None,
            official_reason="Regular NYMEX WTI electronic session.",
            next_open_local=None,
            source=str(schedule["source"]),
            venue_dt=venue_dt,
            local_dt=local_dt,
        )

    def _load_us_equities_schedule(self, year: int) -> dict[str, object]:
        cache_name = f"us_equities_cash_{year}.json"
        cached = self._read_cache(cache_name)
        ttl_hours = self.CACHE_TTL_HOURS["us_equities_cash"]

        if self._is_cache_fresh(cached, ttl_hours=ttl_hours):
            return cached["payload"]

        try:
            payload = self._build_us_equities_schedule_from_remote(year)
            self._write_cache(cache_name, payload)
            return payload
        except Exception:
            if cached is not None:
                return cached["payload"]

            if year == 2026:
                return dict(self.STATIC_US_EQUITIES_2026)

            raise

    def _load_euronext_schedule(self, year: int) -> dict[str, object]:
        cache_name = f"euronext_cash_{year}.json"
        cached = self._read_cache(cache_name)
        ttl_hours = self.CACHE_TTL_HOURS["euronext_cash"]

        if self._is_cache_fresh(cached, ttl_hours=ttl_hours):
            return cached["payload"]

        try:
            payload = self._build_euronext_schedule_from_remote(year)
            self._write_cache(cache_name, payload)
            return payload
        except Exception:
            if cached is not None:
                return cached["payload"]
            raise

    def _load_wti_schedule(self, year: int) -> dict[str, object]:
        cache_name = f"nymex_wti_{year}.json"
        cached = self._read_cache(cache_name)
        ttl_hours = self.CACHE_TTL_HOURS["nymex_wti"]

        if self._is_cache_fresh(cached, ttl_hours=ttl_hours):
            return cached["payload"]

        try:
            payload = self._build_wti_schedule_from_remote(year)
            self._write_cache(cache_name, payload)
            return payload
        except Exception:
            if cached is not None:
                return cached["payload"]
            return dict(self.STATIC_WTI_SCHEDULE)

    def _build_us_equities_schedule_from_remote(self, year: int) -> dict[str, object]:
        last_error: Exception | None = None

        for source_name, url in self.US_EQUITIES_SOURCES:
            try:
                html_text = self.fetcher(url)
                parsed = self._parse_us_equities_html(
                    html_text=html_text,
                    year=year,
                    source_name=source_name,
                )
                if parsed["holidays"] or parsed["early_closes"]:
                    return parsed
            except Exception as exc:
                last_error = exc

        if last_error is not None:
            raise last_error

        raise ValueError(f"Unable to build U.S. equities schedule for year {year}.")

    def _build_euronext_schedule_from_remote(self, year: int) -> dict[str, object]:
        source_name, url = self.EURONEXT_SOURCE
        html_text = self.fetcher(url)
        return self._parse_euronext_html(
            html_text=html_text,
            year=year,
            source_name=source_name,
        )

    def _build_wti_schedule_from_remote(self, year: int) -> dict[str, object]:
        source_name, url = self.WTI_SOURCE
        html_text = self.fetcher(url)
        return self._parse_wti_html(
            html_text=html_text,
            year=year,
            source_name=source_name,
        )

    def _parse_us_equities_html(
        self,
        *,
        html_text: str,
        year: int,
        source_name: str,
    ) -> dict[str, object]:
        tables = self._read_html_tables(html_text)
        holidays: dict[str, str] = {}
        early_closes: dict[str, dict[str, str]] = {}

        for table in tables:
            normalized_columns = {
                original: self._normalize_column_name(original) for original in table.columns
            }

            holiday_col = self._find_column(normalized_columns, {"holiday"})
            date_col = self._find_column(normalized_columns, {"date"})
            status_col = self._find_column(
                normalized_columns,
                {"status", "market_status", "marketstatus"},
            )

            if holiday_col is None or date_col is None or status_col is None:
                continue

            for _, row in table.iterrows():
                holiday_name = self._clean_text(row.get(holiday_col))
                date_text = self._clean_text(row.get(date_col))
                status_text = self._clean_text(row.get(status_col))

                if not holiday_name or not date_text or not status_text:
                    continue

                parsed_date = self._parse_date_text(date_text, year)
                if parsed_date is None:
                    continue

                status_lower = status_text.lower()
                holiday_lower = holiday_name.lower()

                if "closed" in status_lower:
                    holidays[parsed_date.isoformat()] = holiday_name
                    continue

                if (
                    "1:00" in status_lower
                    or "1pm" in status_lower
                    or "early close" in holiday_lower
                ):
                    resolved_name = holiday_name
                    if resolved_name.lower() == "early close":
                        resolved_name = "Early Close"

                    early_closes[parsed_date.isoformat()] = {
                        "name": resolved_name,
                        "close_time": "13:00",
                    }

        if not holidays and not early_closes:
            raise ValueError(f"No usable U.S. equities schedule rows found for year {year}.")

        return {
            "holidays": holidays,
            "early_closes": early_closes,
            "source": source_name,
        }

    def _parse_euronext_html(
        self,
        *,
        html_text: str,
        year: int,
        source_name: str,
    ) -> dict[str, object]:
        tables = self._read_html_tables(html_text)
        holidays: dict[str, str] = {}
        early_closes: dict[str, dict[str, str]] = {}

        for table in tables:
            if table.empty:
                continue

            first_column = table.columns[0]

            for _, row in table.iterrows():
                first_value = self._clean_text(row.get(first_column))
                row_values = [self._clean_text(value) for value in row.tolist()]
                row_text = " | ".join(value for value in row_values if value)

                if str(year) not in row_text and str(year) not in first_value:
                    continue

                parsed_date = self._extract_calendar_date(row_text, year)
                if parsed_date is None:
                    continue

                row_text_lower = row_text.lower()

                if "half trading day" in row_text_lower:
                    early_closes[parsed_date.isoformat()] = {
                        "name": first_value or f"Euronext half trading day {parsed_date.isoformat()}",
                        "close_time": "half_day",
                    }

                if " closed" in f" {row_text_lower} ":
                    holidays[parsed_date.isoformat()] = (
                        first_value or f"Euronext holiday {parsed_date.isoformat()}"
                    )

        if not holidays and not early_closes:
            raise ValueError(f"No usable Euronext schedule rows found for year {year}.")

        return {
            "holidays": holidays,
            "early_closes": early_closes,
            "source": source_name,
        }

    def _parse_wti_html(
        self,
        *,
        html_text: str,
        year: int,
        source_name: str,
    ) -> dict[str, object]:
        tables = self._read_html_tables(html_text)
        special_sessions: dict[str, dict[str, str]] = {}

        for table in tables:
            normalized_columns = {
                original: self._normalize_column_name(original) for original in table.columns
            }

            holiday_col = self._find_column(normalized_columns, {"holiday", "event", "name"})
            date_col = self._find_column(normalized_columns, {"date"})
            status_col = self._find_column(
                normalized_columns,
                {"status", "market_status", "marketstatus", "session_status", "trading_status"},
            )
            close_col = self._find_column(
                normalized_columns,
                {"close", "close_time", "session_close", "trading_close"},
            )
            next_open_col = self._find_column(
                normalized_columns,
                {"next_open", "next_open_time", "resume_time", "reopen_time", "session_open"},
            )

            if holiday_col is None or date_col is None or status_col is None:
                continue

            for _, row in table.iterrows():
                holiday_name = self._clean_text(row.get(holiday_col))
                date_text = self._clean_text(row.get(date_col))
                status_text = self._clean_text(row.get(status_col))

                if not holiday_name or not date_text or not status_text:
                    continue

                parsed_date = self._parse_date_text(date_text, year)
                if parsed_date is None:
                    continue

                market_status = self._normalize_wti_market_status(status_text)
                if market_status is None:
                    continue

                close_time = self._extract_time_value(self._clean_text(row.get(close_col)))
                next_open_text = self._clean_text(row.get(next_open_col))

                special_sessions[parsed_date.isoformat()] = {
                    "name": holiday_name,
                    "market_status": market_status,
                    "close_time": close_time or "",
                    "next_open": next_open_text,
                }

        if not special_sessions:
            raise ValueError(f"No usable WTI schedule rows found for year {year}.")

        return {
            "special_sessions": special_sessions,
            "source": source_name,
        }

    def _next_us_equities_open_local(
        self,
        *,
        from_venue_dt: datetime,
        timezone_name: str,
    ) -> str:
        candidate_date = from_venue_dt.date()

        if from_venue_dt.time() >= self.US_EQUITIES_OPEN:
            candidate_date += timedelta(days=1)

        while True:
            schedule = self._load_us_equities_schedule(candidate_date.year)

            if candidate_date.weekday() >= 5:
                candidate_date += timedelta(days=1)
                continue

            if candidate_date.isoformat() in schedule["holidays"]:
                candidate_date += timedelta(days=1)
                continue

            candidate_dt = datetime.combine(
                candidate_date,
                self.US_EQUITIES_OPEN,
                tzinfo=self.US_EQUITIES_TZ,
            )
            return candidate_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

    def _next_wti_open_local(
        self,
        *,
        from_venue_dt: datetime,
        timezone_name: str,
    ) -> str:
        weekday = from_venue_dt.weekday()
        venue_time = from_venue_dt.time()

        if weekday == 5:
            target_dt = datetime.combine(
                from_venue_dt.date() + timedelta(days=1),
                self.WTI_OPEN_SUNDAY,
                tzinfo=self.WTI_TZ,
            )
            return target_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

        if weekday == 6 and venue_time < self.WTI_OPEN_SUNDAY:
            target_dt = datetime.combine(
                from_venue_dt.date(),
                self.WTI_OPEN_SUNDAY,
                tzinfo=self.WTI_TZ,
            )
            return target_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

        if weekday == 4 and venue_time >= self.WTI_FRIDAY_CLOSE:
            target_dt = datetime.combine(
                from_venue_dt.date() + timedelta(days=2),
                self.WTI_OPEN_SUNDAY,
                tzinfo=self.WTI_TZ,
            )
            return target_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

        if self.WTI_DAILY_BREAK_START <= venue_time < self.WTI_DAILY_BREAK_END:
            target_dt = datetime.combine(
                from_venue_dt.date(),
                self.WTI_DAILY_BREAK_END,
                tzinfo=self.WTI_TZ,
            )
            return target_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

        return from_venue_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

    def _resolve_wti_special_next_open_local(
        self,
        *,
        special_session: dict[str, str],
        from_venue_dt: datetime,
        timezone_name: str,
    ) -> str | None:
        next_open_text = self._clean_text(special_session.get("next_open"))
        if next_open_text:
            parsed_next_open = self._parse_datetime_text(
                next_open_text,
                default_year=from_venue_dt.year,
                tzinfo=self.WTI_TZ,
            )
            if parsed_next_open is not None:
                return parsed_next_open.astimezone(ZoneInfo(timezone_name)).isoformat()

            parsed_time = self._parse_flexible_time(next_open_text)
            if parsed_time is not None:
                target_date = from_venue_dt.date()
                if parsed_time <= from_venue_dt.time():
                    target_date += timedelta(days=1)
                target_dt = datetime.combine(target_date, parsed_time, tzinfo=self.WTI_TZ)
                return target_dt.astimezone(ZoneInfo(timezone_name)).isoformat()

        return self._next_wti_open_local(
            from_venue_dt=from_venue_dt,
            timezone_name=timezone_name,
        )

    def _read_cache(self, cache_name: str) -> dict[str, object] | None:
        cache_path = self.cache_dir / cache_name
        if not cache_path.exists():
            return None

        return json.loads(cache_path.read_text(encoding="utf-8"))

    def _write_cache(self, cache_name: str, payload: dict[str, object]) -> None:
        cache_path = self.cache_dir / cache_name
        wrapped = {
            "fetched_at_utc": self.now_provider().isoformat(),
            "payload": payload,
        }
        cache_path.write_text(
            json.dumps(wrapped, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _is_cache_fresh(
        self,
        cached: dict[str, object] | None,
        *,
        ttl_hours: int,
    ) -> bool:
        if cached is None:
            return False

        fetched_at_raw = cached.get("fetched_at_utc")
        if not fetched_at_raw:
            return False

        fetched_at = datetime.fromisoformat(str(fetched_at_raw))
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        age = self.now_provider() - fetched_at.astimezone(timezone.utc)
        return age <= timedelta(hours=ttl_hours)

    def _default_fetcher(self, url: str) -> str:
        request = Request(
            url,
            headers={"User-Agent": "EYE/1.0 (market-calendar; +local-project)"},
        )
        with urlopen(request, timeout=20) as response:
            return response.read().decode("utf-8", errors="replace")

    def _read_html_tables(self, html_text: str) -> list[pd.DataFrame]:
        try:
            return pd.read_html(StringIO(html_text))
        except Exception:
            return self._parse_html_tables_fallback(html_text)

    def _parse_html_tables_fallback(self, html_text: str) -> list[pd.DataFrame]:
        tables: list[pd.DataFrame] = []

        for table_html in re.findall(r"<table\b.*?>.*?</table>", html_text, flags=re.IGNORECASE | re.DOTALL):
            headers: list[str] | None = None
            rows: list[list[str]] = []

            for row_html in re.findall(r"<tr\b.*?>.*?</tr>", table_html, flags=re.IGNORECASE | re.DOTALL):
                header_cells = re.findall(
                    r"<th\b.*?>(.*?)</th>",
                    row_html,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if header_cells and headers is None:
                    headers = [self._html_cell_text(cell) for cell in header_cells]
                    continue

                data_cells = re.findall(
                    r"<td\b.*?>(.*?)</td>",
                    row_html,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if data_cells:
                    rows.append([self._html_cell_text(cell) for cell in data_cells])

            if not rows:
                continue

            if headers is None:
                width = max(len(row) for row in rows)
                headers = [f"column_{index}" for index in range(width)]

            normalized_rows = [
                row + [""] * (len(headers) - len(row)) if len(row) < len(headers) else row[: len(headers)]
                for row in rows
            ]
            tables.append(pd.DataFrame(normalized_rows, columns=headers))

        if not tables:
            raise ValueError("No HTML tables could be parsed from source page.")

        return tables

    def _html_cell_text(self, value: str) -> str:
        text = re.sub(r"<br\s*/?>", " ", value, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        return self._clean_text(unescape(text))

    def _normalize_asset(self, asset: MarketSymbol | str) -> str:
        return str(getattr(asset, "value", asset)).strip().lower()

    def _normalize_current_dt(self, current_dt: datetime | None) -> datetime:
        if current_dt is None:
            return self.now_provider()

        if current_dt.tzinfo is None:
            return current_dt.replace(tzinfo=timezone.utc)

        return current_dt.astimezone(timezone.utc)

    def _normalize_column_name(self, value) -> str:
        text = self._clean_text(value).lower()
        text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
        return text

    def _find_column(
        self,
        normalized_columns: dict[object, str],
        accepted_names: set[str],
    ):
        for original, normalized in normalized_columns.items():
            if normalized in accepted_names:
                return original
        return None

    def _clean_text(self, value) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        return str(value).strip()

    def _parse_date_text(self, date_text: str, year: int) -> date | None:
        clean_text = re.sub(r"\s+", " ", date_text).strip()
        if not clean_text:
            return None

        if not re.search(r"\b20\d{2}\b", clean_text):
            clean_text = f"{clean_text} {year}"

        parsed = pd.to_datetime(clean_text, errors="coerce")
        if pd.isna(parsed):
            return None

        return parsed.date()

    def _extract_calendar_date(self, text: str, year: int) -> date | None:
        match = re.search(
            rf"([A-Za-z]+day)?\s*(\d{{1,2}})\s+([A-Za-z]+)\s+{year}",
            text,
            flags=re.IGNORECASE,
        )
        if match is None:
            alt_match = re.search(
                rf"([A-Za-z]+)\s+(\d{{1,2}}),?\s+{year}",
                text,
                flags=re.IGNORECASE,
            )
            if alt_match is None:
                return None
            candidate = f"{alt_match.group(1)} {alt_match.group(2)} {year}"
        else:
            candidate = f"{match.group(3)} {match.group(2)} {year}"

        parsed = pd.to_datetime(candidate, errors="coerce")
        if pd.isna(parsed):
            return None

        return parsed.date()

    def _parse_datetime_text(
        self,
        value: str,
        *,
        default_year: int,
        tzinfo: ZoneInfo,
    ) -> datetime | None:
        clean_text = re.sub(r"\s+", " ", value).strip()
        if not clean_text:
            return None

        if not re.search(r"\b20\d{2}\b", clean_text):
            clean_text = f"{clean_text} {default_year}"

        parsed = pd.to_datetime(clean_text, errors="coerce")
        if pd.isna(parsed):
            return None

        python_dt = parsed.to_pydatetime()
        if python_dt.tzinfo is None:
            python_dt = python_dt.replace(tzinfo=tzinfo)
        else:
            python_dt = python_dt.astimezone(tzinfo)

        return python_dt

    def _extract_time_value(self, value: str) -> str | None:
        clean_text = self._clean_text(value)
        if not clean_text:
            return None

        parsed_time = self._parse_flexible_time(clean_text)
        if parsed_time is None:
            return None

        return parsed_time.strftime("%H:%M")

    def _parse_flexible_time(self, value) -> time | None:
        clean_text = self._clean_text(value)
        if not clean_text:
            return None

        if re.fullmatch(r"\d{1,2}:\d{2}", clean_text):
            return self._parse_hhmm(clean_text)

        parsed = pd.to_datetime(clean_text, errors="coerce")
        if pd.isna(parsed):
            return None

        return parsed.time()

    def _normalize_wti_market_status(self, value: str) -> str | None:
        status_lower = self._clean_text(value).lower()

        if "closed" in status_lower:
            return "closed_holiday"
        if "reduced" in status_lower or "shortened" in status_lower:
            return "reduced_hours"
        if "early" in status_lower and "close" in status_lower:
            return "early_close"

        return None

    def _parse_hhmm(self, value: str) -> time:
        hour, minute = value.split(":", 1)
        return time(int(hour), int(minute))

    def _build_result(
        self,
        *,
        asset: str,
        venue: str,
        market_status: str,
        is_open_now: bool,
        is_tradable_now: bool,
        holiday_name: str | None,
        official_reason: str,
        next_open_local: str | None,
        source: str,
        venue_dt: datetime,
        local_dt: datetime,
    ) -> dict[str, object]:
        return {
            "asset": asset.upper(),
            "venue": venue,
            "market_status": market_status,
            "is_open_now": is_open_now,
            "is_tradable_now": is_tradable_now,
            "holiday_name": holiday_name,
            "official_reason": official_reason,
            "next_open_local": next_open_local,
            "source": source,
            "venue_time": venue_dt.isoformat(),
            "local_time": local_dt.isoformat(),
        }
