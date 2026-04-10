from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from maintenance_storage import (
    DB_PATH,
    POWER_SOURCES,
    fetch_diesel_entries,
    fetch_power_readings,
    initialize_database,
    save_diesel_entry,
    save_power_readings,
)


WEEKDAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def parse_optional_float(raw_value: str, label: str) -> float | None:
    value = raw_value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"{label} must be a valid number.") from error


def parse_required_float(raw_value: str, label: str) -> float:
    parsed = parse_optional_float(raw_value, label)
    if parsed is None:
        raise ValueError(f"{label} is required.")
    return parsed


def source_key(source: str) -> str:
    return source.lower().replace(" ", "_")


def build_power_entry_form() -> None:
    st.subheader("Power Inputs")
    st.caption(
        "Save readings for Mains and Generators independently. "
        "Day KWHr is stored as 8am KWHr - 6pm KWHr, following your requested formula."
    )

    with st.form("power_entry_form"):
        date_column, day_column = st.columns(2)
        record_date = date_column.date_input("Date", value=date.today(), key="power_date")
        default_day_index = WEEKDAYS.index(record_date.strftime("%A"))
        day_name = day_column.selectbox(
            "Day",
            options=WEEKDAYS,
            index=default_day_index,
            key="power_day_name",
        )

        power_inputs: dict[str, dict[str, str]] = {}

        for source in POWER_SOURCES:
            key_prefix = source_key(source)
            with st.expander(source, expanded=source == "Mains"):
                input_columns = st.columns(3)
                power_inputs[source] = {
                    "eight_am_kwh": input_columns[0].text_input(
                        "8am KWHr",
                        key=f"{key_prefix}_8am_kwh",
                        placeholder="e.g. 1250",
                    ),
                    "six_pm_kwh": input_columns[1].text_input(
                        "6pm KWHr",
                        key=f"{key_prefix}_6pm_kwh",
                        placeholder="e.g. 1320",
                    ),
                    "run_hour": input_columns[2].text_input(
                        "Run Hour",
                        key=f"{key_prefix}_run_hour",
                        placeholder="e.g. 10",
                    ),
                }
                st.caption("Night KWHr is filled automatically when the next day's 8am reading is saved.")

        submitted = st.form_submit_button("Save Power Readings", use_container_width=True)

    if not submitted:
        return

    errors: list[str] = []
    readings_to_save: list[dict[str, float | str]] = []

    for source in POWER_SOURCES:
        entry = power_inputs[source]
        if not any(value.strip() for value in entry.values()):
            continue

        try:
            readings_to_save.append(
                {
                    "source": source,
                    "eight_am_kwh": parse_required_float(entry["eight_am_kwh"], f"{source} 8am KWHr"),
                    "six_pm_kwh": parse_required_float(entry["six_pm_kwh"], f"{source} 6pm KWHr"),
                    "run_hour": parse_required_float(entry["run_hour"], f"{source} Run Hour"),
                }
            )
        except ValueError as error:
            errors.append(str(error))

    if not readings_to_save:
        errors.append("Enter at least one complete power reading before saving.")

    if errors:
        for error in errors:
            st.error(error)
        return

    save_power_readings(day_name, record_date.isoformat(), readings_to_save)
    st.success("Power readings saved successfully.")


def build_diesel_entry_form() -> None:
    st.subheader("Diesel Inputs")
    st.caption("Diesel records can be saved separately from the generator and mains readings.")

    with st.form("diesel_entry_form"):
        date_column, day_column = st.columns(2)
        record_date = date_column.date_input("Date", value=date.today(), key="diesel_date")
        default_day_index = WEEKDAYS.index(record_date.strftime("%A"))
        day_name = day_column.selectbox(
            "Day",
            options=WEEKDAYS,
            index=default_day_index,
            key="diesel_day_name",
        )

        diesel_columns = st.columns(2)
        estimated_remaining = diesel_columns[0].text_input(
            "Estimated Diesel Remaining",
            key="estimated_diesel_remaining",
            placeholder="e.g. 480",
        )
        diesel_supply = diesel_columns[1].text_input(
            "Diesel Supply",
            key="diesel_supply",
            placeholder="e.g. 100",
        )
        estimated_used_day = diesel_columns[0].text_input(
            "Estimated Diesel Used from 8am to 6pm",
            key="estimated_diesel_used_day",
            placeholder="e.g. 40",
        )
        estimated_used_night = diesel_columns[1].text_input(
            "Estimated Diesel Used from 6pm to 8am",
            key="estimated_diesel_used_night",
            placeholder="e.g. 35",
        )
        diesel_pumped = st.text_input(
            "Diesel Pumped",
            key="diesel_pumped",
            placeholder="e.g. 60",
        )

        submitted = st.form_submit_button("Save Diesel Record", use_container_width=True)

    if not submitted:
        return

    if not any(
        value.strip()
        for value in [
            estimated_remaining,
            diesel_supply,
            estimated_used_day,
            estimated_used_night,
            diesel_pumped,
        ]
    ):
        st.error("Enter at least one diesel value before saving.")
        return

    try:
        save_diesel_entry(
            day_name=day_name,
            record_date=record_date.isoformat(),
            estimated_diesel_remaining=parse_optional_float(
                estimated_remaining,
                "Estimated Diesel Remaining",
            ),
            diesel_supply=parse_optional_float(diesel_supply, "Diesel Supply"),
            estimated_diesel_used_day=parse_optional_float(
                estimated_used_day,
                "Estimated Diesel Used from 8am to 6pm",
            ),
            estimated_diesel_used_night=parse_optional_float(
                estimated_used_night,
                "Estimated Diesel Used from 6pm to 8am",
            ),
            diesel_pumped=parse_optional_float(diesel_pumped, "Diesel Pumped"),
        )
    except ValueError as error:
        st.error(str(error))
        return

    st.success("Diesel record saved successfully.")


def build_history_view() -> None:
    st.subheader("Saved Records")

    power_records = fetch_power_readings()
    diesel_records = fetch_diesel_entries()

    power_column, diesel_column = st.columns(2)

    with power_column:
        st.markdown("**Recent Power Readings**")
        if power_records:
            st.dataframe(pd.DataFrame(power_records), use_container_width=True, hide_index=True)
        else:
            st.info("No power readings saved yet.")

    with diesel_column:
        st.markdown("**Recent Diesel Records**")
        if diesel_records:
            st.dataframe(pd.DataFrame(diesel_records), use_container_width=True, hide_index=True)
        else:
            st.info("No diesel records saved yet.")


def main() -> None:
    st.set_page_config(page_title="Maintenance Record App", layout="wide")
    initialize_database()

    st.title("Maintenance Record App")
    st.caption(f"SQLite database file: {DB_PATH.name}")

    power_tab, diesel_tab, history_tab = st.tabs(
        ["Power Readings", "Diesel", "Records"]
    )

    with power_tab:
        build_power_entry_form()

    with diesel_tab:
        build_diesel_entry_form()

    with history_tab:
        build_history_view()


if __name__ == "__main__":
    main()
