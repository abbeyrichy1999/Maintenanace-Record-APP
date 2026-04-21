from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable
import streamlit as st

from supabase import Client, create_client


ENV_PATH = Path(__file__).with_name(".env")
DATA_BACKEND_NAME = "Supabase"
POWER_SOURCES = ("Mains", "Generator 1", "Generator 2", "Generator 3")


@lru_cache(maxsize=1)
def _load_env_values() -> dict[str, str]:
    if not ENV_PATH.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _get_env_value(*keys: str) -> str | None:
    env_values = _load_env_values()
    for key in keys:
        value = os.getenv(key) or env_values.get(key)
        if value:
            return value
    return None



@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    supabase_url = _get_env_value("SUPABASE_URL", "SUPABASE URL") or st.secrets["SUPABASE_URL"]
    supabase_key = _get_env_value("SUPABASE_KEY", "SUPABASE KEY") or st.secrets["SUPABASE_KEY"]
    if not supabase_url or not supabase_key:
        raise RuntimeError(
            "Supabase credentials are missing. Add SUPABASE_URL and SUPABASE_KEY, or keep their current spaced equivalents, in .env."
        )
    return create_client(supabase_url, supabase_key)


def _execute(query: Any, *, action: str) -> Any:
    
    try:
        return query.execute()
    except Exception as error:
        raise RuntimeError(f"Unable to {action}.") from error


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def initialize_database() -> None:
    get_supabase_client()


def _record_date_with_offset(record_date: str, day_offset: int) -> str:
    return (date.fromisoformat(record_date) + timedelta(days=day_offset)).isoformat()


def _ensure_maintenance_day(day_name: str, record_date: str) -> int:
    client = get_supabase_client()
    _execute(
        client.table("maintenance_days").upsert(
            {
                "day_name": day_name,
                "record_date": record_date,
                "updated_at": _utc_timestamp(),
            },
            on_conflict="record_date",
        ),
        action="save the maintenance day in Supabase",
    )
    response = _execute(
        client.table("maintenance_days")
        .select("id")
        .eq("record_date", record_date)
        .limit(1),
        action="load the maintenance day from Supabase",
    )
    rows = response.data or []
    if not rows:
        raise RuntimeError("Failed to create or load the maintenance day.")
    return int(rows[0]["id"])


def _fetch_maintenance_day(record_date: str) -> dict[str, Any] | None:
    client = get_supabase_client()
    response = _execute(
        client.table("maintenance_days")
        .select("id,day_name,record_date")
        .eq("record_date", record_date)
        .limit(1),
        action="load maintenance day details from Supabase",
    )
    rows = response.data or []
    return rows[0] if rows else None


def _fetch_power_reading(maintenance_day_id: int, source: str) -> dict[str, Any] | None:
    client = get_supabase_client()
    response = _execute(
        client.table("power_readings")
        .select("id,eight_am_kwh,six_pm_kwh")
        .eq("maintenance_day_id", maintenance_day_id)
        .eq("source", source)
        .limit(1),
        action="load power readings from Supabase",
    )
    rows = response.data or []
    return rows[0] if rows else None


def _update_power_night_values(
    reading_id: int,
    next_day_eight_am_kwh: float | None,
    night_kwh: float | None,
) -> None:
    client = get_supabase_client()
    _execute(
        client.table("power_readings")
        .update(
            {
                "next_day_eight_am_kwh": next_day_eight_am_kwh,
                "night_kwh": night_kwh,
                "updated_at": _utc_timestamp(),
            }
        )
        .eq("id", reading_id),
        action="update linked night readings in Supabase",
    )


def _refresh_neighbor_night_values(
    source: str,
    maintenance_day_id: int,
    record_date: str,
) -> None:
    current = _fetch_power_reading(maintenance_day_id, source)
    if current is None:
        return

    previous_day = _fetch_maintenance_day(_record_date_with_offset(record_date, -1))
    if previous_day is not None:
        previous = _fetch_power_reading(int(previous_day["id"]), source)
        if previous is not None:
            current_eight_am = float(current["eight_am_kwh"])
            previous_six_pm = float(previous["six_pm_kwh"])
            _update_power_night_values(
                int(previous["id"]),
                current_eight_am,
                previous_six_pm - current_eight_am,
            )

    next_day = _fetch_maintenance_day(_record_date_with_offset(record_date, 1))
    if next_day is None:
        _update_power_night_values(int(current["id"]), None, None)
        return

    next_reading = _fetch_power_reading(int(next_day["id"]), source)
    if next_reading is None:
        _update_power_night_values(int(current["id"]), None, None)
        return

    current_six_pm = float(current["six_pm_kwh"])
    next_eight_am = float(next_reading["eight_am_kwh"])
    _update_power_night_values(
        int(current["id"]),
        next_eight_am,
        current_six_pm - next_eight_am,
    )


def save_power_readings(
    day_name: str,
    record_date: str,
    readings: Iterable[dict[str, float | str]],
) -> None:
    readings_to_save = list(readings)
    if not readings_to_save:
        return

    client = get_supabase_client()
    maintenance_day_id = _ensure_maintenance_day(day_name, record_date)
    timestamp = _utc_timestamp()
    payloads: list[dict[str, float | int | str]] = []

    for reading in readings_to_save:
        eight_am_kwh = float(reading["eight_am_kwh"])
        six_pm_kwh = float(reading["six_pm_kwh"])
        run_hour = float(reading["run_hour"])
        source = str(reading["source"])

        payloads.append(
            {
                "maintenance_day_id": maintenance_day_id,
                "source": source,
                "eight_am_kwh": eight_am_kwh,
                "six_pm_kwh": six_pm_kwh,
                "day_kwh": eight_am_kwh - six_pm_kwh,
                "run_hour": run_hour,
                "updated_at": timestamp,
            }
        )

    _execute(
        client.table("power_readings").upsert(
            payloads,
            on_conflict="maintenance_day_id,source",
        ),
        action="save power readings to Supabase",
    )

    for reading in readings_to_save:
        _refresh_neighbor_night_values(
            source=str(reading["source"]),
            maintenance_day_id=maintenance_day_id,
            record_date=record_date,
        )


def save_diesel_entry(
    day_name: str,
    record_date: str,
    estimated_diesel_remaining: float | None,
    diesel_supply: float | None,
    estimated_diesel_used_day: float | None,
    estimated_diesel_used_night: float | None,
    diesel_pumped: float | None,
) -> None:
    client = get_supabase_client()
    maintenance_day_id = _ensure_maintenance_day(day_name, record_date)
    response = _execute(
        client.table("diesel_entries")
        .select(
            "estimated_diesel_remaining,diesel_supply,estimated_diesel_used_day,"
            "estimated_diesel_used_night,diesel_pumped"
        )
        .eq("maintenance_day_id", maintenance_day_id)
        .limit(1),
        action="load existing diesel entries from Supabase",
    )
    existing_rows = response.data or []
    existing_entry = existing_rows[0] if existing_rows else {}

    _execute(
        client.table("diesel_entries").upsert(
            {
                "maintenance_day_id": maintenance_day_id,
                "estimated_diesel_remaining": (
                    estimated_diesel_remaining
                    if estimated_diesel_remaining is not None
                    else existing_entry.get("estimated_diesel_remaining")
                ),
                "diesel_supply": (
                    diesel_supply
                    if diesel_supply is not None
                    else existing_entry.get("diesel_supply")
                ),
                "estimated_diesel_used_day": (
                    estimated_diesel_used_day
                    if estimated_diesel_used_day is not None
                    else existing_entry.get("estimated_diesel_used_day")
                ),
                "estimated_diesel_used_night": (
                    estimated_diesel_used_night
                    if estimated_diesel_used_night is not None
                    else existing_entry.get("estimated_diesel_used_night")
                ),
                "diesel_pumped": (
                    diesel_pumped
                    if diesel_pumped is not None
                    else existing_entry.get("diesel_pumped")
                ),
                "updated_at": _utc_timestamp(),
            },
            on_conflict="maintenance_day_id",
        ),
        action="save diesel entries to Supabase",
    )


def _fetch_recent_maintenance_days(limit: int) -> list[dict[str, Any]]:
    client = get_supabase_client()
    response = _execute(
        client.table("maintenance_days")
        .select("id,day_name,record_date")
        .order("record_date", desc=True)
        .limit(limit),
        action="load recent maintenance days from Supabase",
    )
    return response.data or []


def fetch_power_readings(limit: int = 50) -> list[dict[str, object]]:
    if limit <= 0:
        return []

    recent_days = _fetch_recent_maintenance_days(max(limit, 100))
    if not recent_days:
        return []

    day_lookup = {int(day["id"]): day for day in recent_days}
    day_ids = list(day_lookup)
    client = get_supabase_client()
    response = _execute(
        client.table("power_readings").select(
            "maintenance_day_id,source,eight_am_kwh,six_pm_kwh,day_kwh,"
            "next_day_eight_am_kwh,night_kwh,run_hour"
        ).in_("maintenance_day_id", day_ids),
        action="load power history from Supabase",
    )

    records: list[dict[str, object]] = []
    for row in response.data or []:
        day = day_lookup.get(int(row["maintenance_day_id"]))
        if day is None:
            continue

        records.append(
            {
                "day": day["day_name"],
                "date": day["record_date"],
                "source": row["source"],
                "8am KWHr": row["eight_am_kwh"],
                "6pm KWHr": row["six_pm_kwh"],
                "Day KWHr": row["day_kwh"],
                "Next Day 8am KWHr": row["next_day_eight_am_kwh"],
                "Night KWHr": row["night_kwh"],
                "Run Hour": row["run_hour"],
            }
        )

    records.sort(key=lambda item: str(item["source"]))
    records.sort(key=lambda item: str(item["date"]), reverse=True)
    return records[:limit]


def fetch_diesel_entries(limit: int = 50) -> list[dict[str, object]]:
    if limit <= 0:
        return []

    recent_days = _fetch_recent_maintenance_days(max(limit, 50))
    if not recent_days:
        return []

    day_lookup = {int(day["id"]): day for day in recent_days}
    day_ids = list(day_lookup)
    client = get_supabase_client()
    response = _execute(
        client.table("diesel_entries").select(
            "maintenance_day_id,estimated_diesel_remaining,diesel_supply,"
            "estimated_diesel_used_day,estimated_diesel_used_night,diesel_pumped"
        ).in_("maintenance_day_id", day_ids),
        action="load diesel history from Supabase",
    )

    records: list[dict[str, object]] = []
    for row in response.data or []:
        day = day_lookup.get(int(row["maintenance_day_id"]))
        if day is None:
            continue

        records.append(
            {
                "day": day["day_name"],
                "date": day["record_date"],
                "Estimated Diesel Remaining": row["estimated_diesel_remaining"],
                "Diesel Supply": row["diesel_supply"],
                "Estimated Diesel Used 8am to 6pm": row["estimated_diesel_used_day"],
                "Estimated Diesel Used 6pm to 8am": row["estimated_diesel_used_night"],
                "Diesel Pumped": row["diesel_pumped"],
            }
        )

    records.sort(key=lambda item: str(item["date"]), reverse=True)
    return records[:limit]
