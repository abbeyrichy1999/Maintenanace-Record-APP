from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable


DB_PATH = Path(__file__).with_name("maintenance_records.db")
POWER_SOURCES = ("Mains", "Generator 1", "Generator 2", "Generator 3")


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def initialize_database() -> None:
    with get_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS maintenance_days (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                day_name TEXT NOT NULL,
                record_date TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS power_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                maintenance_day_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                eight_am_kwh REAL NOT NULL,
                six_pm_kwh REAL NOT NULL,
                day_kwh REAL NOT NULL,
                next_day_eight_am_kwh REAL,
                night_kwh REAL,
                run_hour REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (maintenance_day_id, source),
                FOREIGN KEY (maintenance_day_id) REFERENCES maintenance_days(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS diesel_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                maintenance_day_id INTEGER NOT NULL UNIQUE,
                estimated_diesel_remaining REAL,
                diesel_supply REAL,
                estimated_diesel_used_day REAL,
                estimated_diesel_used_night REAL,
                diesel_pumped REAL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (maintenance_day_id) REFERENCES maintenance_days(id) ON DELETE CASCADE
            );
            """
        )


def _ensure_maintenance_day(
    connection: sqlite3.Connection,
    day_name: str,
    record_date: str,
) -> int:
    connection.execute(
        """
        INSERT INTO maintenance_days (day_name, record_date, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(record_date) DO UPDATE SET
            day_name = excluded.day_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (day_name, record_date),
    )
    row = connection.execute(
        "SELECT id FROM maintenance_days WHERE record_date = ?",
        (record_date,),
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to create or load the maintenance day.")
    return int(row["id"])


def _refresh_neighbor_night_values(
    connection: sqlite3.Connection,
    source: str,
    maintenance_day_id: int,
) -> None:
    current = connection.execute(
        """
        SELECT
            pr.id,
            pr.eight_am_kwh,
            pr.six_pm_kwh,
            md.record_date
        FROM power_readings pr
        JOIN maintenance_days md ON md.id = pr.maintenance_day_id
        WHERE pr.maintenance_day_id = ? AND pr.source = ?
        """,
        (maintenance_day_id, source),
    ).fetchone()
    if current is None:
        return

    previous = connection.execute(
        """
        SELECT
            pr.id,
            pr.six_pm_kwh
        FROM power_readings pr
        JOIN maintenance_days md ON md.id = pr.maintenance_day_id
        WHERE pr.source = ? AND md.record_date = date(?, '-1 day')
        """,
        (source, current["record_date"]),
    ).fetchone()

    if previous is not None:
        connection.execute(
            """
            UPDATE power_readings
            SET
                next_day_eight_am_kwh = ?,
                night_kwh = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                current["eight_am_kwh"],
                float(previous["six_pm_kwh"]) - float(current["eight_am_kwh"]),
                previous["id"],
            ),
        )

    next_row = connection.execute(
        """
        SELECT
            pr.id,
            pr.eight_am_kwh
        FROM power_readings pr
        JOIN maintenance_days md ON md.id = pr.maintenance_day_id
        WHERE pr.source = ? AND md.record_date = date(?, '+1 day')
        """,
        (source, current["record_date"]),
    ).fetchone()

    if next_row is None:
        connection.execute(
            """
            UPDATE power_readings
            SET
                next_day_eight_am_kwh = NULL,
                night_kwh = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (current["id"],),
        )
        return

    connection.execute(
        """
        UPDATE power_readings
        SET
            next_day_eight_am_kwh = ?,
            night_kwh = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            next_row["eight_am_kwh"],
            float(current["six_pm_kwh"]) - float(next_row["eight_am_kwh"]),
            current["id"],
        ),
    )


def save_power_readings(
    day_name: str,
    record_date: str,
    readings: Iterable[dict[str, float | str]],
) -> None:
    with get_connection() as connection:
        maintenance_day_id = _ensure_maintenance_day(connection, day_name, record_date)

        for reading in readings:
            eight_am_kwh = float(reading["eight_am_kwh"])
            six_pm_kwh = float(reading["six_pm_kwh"])
            run_hour = float(reading["run_hour"])
            source = str(reading["source"])
            day_kwh = eight_am_kwh - six_pm_kwh

            connection.execute(
                """
                INSERT INTO power_readings (
                    maintenance_day_id,
                    source,
                    eight_am_kwh,
                    six_pm_kwh,
                    day_kwh,
                    run_hour,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(maintenance_day_id, source) DO UPDATE SET
                    eight_am_kwh = excluded.eight_am_kwh,
                    six_pm_kwh = excluded.six_pm_kwh,
                    day_kwh = excluded.day_kwh,
                    run_hour = excluded.run_hour,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    maintenance_day_id,
                    source,
                    eight_am_kwh,
                    six_pm_kwh,
                    day_kwh,
                    run_hour,
                ),
            )
            _refresh_neighbor_night_values(connection, source, maintenance_day_id)

        connection.commit()


def save_diesel_entry(
    day_name: str,
    record_date: str,
    estimated_diesel_remaining: float | None,
    diesel_supply: float | None,
    estimated_diesel_used_day: float | None,
    estimated_diesel_used_night: float | None,
    diesel_pumped: float | None,
) -> None:
    with get_connection() as connection:
        maintenance_day_id = _ensure_maintenance_day(connection, day_name, record_date)
        connection.execute(
            """
            INSERT INTO diesel_entries (
                maintenance_day_id,
                estimated_diesel_remaining,
                diesel_supply,
                estimated_diesel_used_day,
                estimated_diesel_used_night,
                diesel_pumped,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(maintenance_day_id) DO UPDATE SET
                estimated_diesel_remaining = COALESCE(
                    excluded.estimated_diesel_remaining,
                    diesel_entries.estimated_diesel_remaining
                ),
                diesel_supply = COALESCE(
                    excluded.diesel_supply,
                    diesel_entries.diesel_supply
                ),
                estimated_diesel_used_day = COALESCE(
                    excluded.estimated_diesel_used_day,
                    diesel_entries.estimated_diesel_used_day
                ),
                estimated_diesel_used_night = COALESCE(
                    excluded.estimated_diesel_used_night,
                    diesel_entries.estimated_diesel_used_night
                ),
                diesel_pumped = COALESCE(
                    excluded.diesel_pumped,
                    diesel_entries.diesel_pumped
                ),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                maintenance_day_id,
                estimated_diesel_remaining,
                diesel_supply,
                estimated_diesel_used_day,
                estimated_diesel_used_night,
                diesel_pumped,
            ),
        )
        connection.commit()


def fetch_power_readings(limit: int = 50) -> list[dict[str, object]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                md.day_name AS day,
                md.record_date AS date,
                pr.source,
                pr.eight_am_kwh AS "8am KWHr",
                pr.six_pm_kwh AS "6pm KWHr",
                pr.day_kwh AS "Day KWHr",
                pr.next_day_eight_am_kwh AS "Next Day 8am KWHr",
                pr.night_kwh AS "Night KWHr",
                pr.run_hour AS "Run Hour"
            FROM power_readings pr
            JOIN maintenance_days md ON md.id = pr.maintenance_day_id
            ORDER BY date(md.record_date) DESC, pr.source ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_diesel_entries(limit: int = 50) -> list[dict[str, object]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                md.day_name AS day,
                md.record_date AS date,
                de.estimated_diesel_remaining AS "Estimated Diesel Remaining",
                de.diesel_supply AS "Diesel Supply",
                de.estimated_diesel_used_day AS "Estimated Diesel Used 8am to 6pm",
                de.estimated_diesel_used_night AS "Estimated Diesel Used 6pm to 8am",
                de.diesel_pumped AS "Diesel Pumped"
            FROM diesel_entries de
            JOIN maintenance_days md ON md.id = de.maintenance_day_id
            ORDER BY date(md.record_date) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]
