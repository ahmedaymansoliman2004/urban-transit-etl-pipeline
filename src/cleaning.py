from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json
from typing import Any

import polars as pl

PROJECT_DIR = Path("/opt/airflow/project")

CLEANING_REPORT_PATH = PROJECT_DIR / "logs" / "cleaning_report.json"
CLEANED_OUTPUT_PATH = PROJECT_DIR / "data" / "interim" / "cleaned_tripdata.csv"

VALID_MEMBER_TYPES = {"member", "casual"}


def create_cleaning_report(initial_rows: int) -> dict[str, Any]:
    """Initialize a dictionary to track data cleaning metrics."""
    return {
        "cleaning_time": datetime.now().isoformat(timespec="seconds"),
        "initial_rows": initial_rows,
        "final_rows": initial_rows,
        "removed_rows": 0,
        "rules_applied": {},
    }


def clean_data(df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Clean the raw dataframe by handling nulls, duplicates, and invalid data."""
    report = create_cleaning_report(df.height)

    start_missing = df["start_station_name"].null_count()
    end_missing = df["end_station_name"].null_count()

    df = df.with_columns([
        pl.col("start_station_name").fill_null("Unknown Station"),
        pl.col("end_station_name").fill_null("Unknown Station"),
    ])
    report["rules_applied"]["missing_station_names"] = start_missing + end_missing

    start_ids = df["start_station_id"].null_count()
    end_ids = df["end_station_id"].null_count()

    df = df.with_columns([
        pl.col("start_station_id").fill_null("Unknown_ID"),
        pl.col("end_station_id").fill_null("Unknown_ID"),
    ])
    report["rules_applied"]["missing_station_ids"] = start_ids + end_ids

    before = df.height
    df = df.drop_nulls(["start_lat", "start_lng", "end_lat", "end_lng"])
    report["rules_applied"]["missing_coordinates_removed"] = before - df.height

    before = df.height
    df = df.unique()
    report["rules_applied"]["duplicate_rows_removed"] = before - df.height

    before = df.height
    df = df.unique(subset=["ride_id"])
    report["rules_applied"]["duplicate_ride_ids_removed"] = before - df.height

    text_columns = ["start_station_name", "end_station_name", "member_casual"]
    for col in text_columns:
        df = df.with_columns(
            pl.col(col)
            .str.strip_chars()
            .str.replace_all(r"\s+", " ")
            .str.to_lowercase()
        )

    before = df.height
    df = df.filter(pl.col("member_casual").is_in(VALID_MEMBER_TYPES))
    report["rules_applied"]["invalid_member_types_removed"] = before - df.height

    df = df.with_columns([
        (pl.col("start_date").cast(pl.Utf8) + " " + pl.col("start_time"))
        .str.strptime(pl.Datetime, "%Y-%m-%d %I:%M:%S %p")
        .alias("start_datetime"),

        (pl.col("end_date").cast(pl.Utf8) + " " + pl.col("end_time"))
        .str.strptime(pl.Datetime, "%Y-%m-%d %I:%M:%S %p")
        .alias("end_datetime"),
    ])

    before = df.height
    df = df.filter(pl.col("end_datetime") > pl.col("start_datetime"))
    report["rules_applied"]["invalid_trips_removed"] = before - df.height

    df = df.drop(["start_datetime", "end_datetime"])

    report["final_rows"] = df.height
    report["removed_rows"] = report["initial_rows"] - report["final_rows"]

    return df, report


def save_cleaning_report(report: dict[str, Any]) -> None:
    """Save the cleaning report to a JSON file."""
    CLEANING_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(CLEANING_REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4, ensure_ascii=False)

    print(f"Cleaning report saved to: {CLEANING_REPORT_PATH}")


def save_cleaned_dataframe(df: pl.DataFrame) -> None:
    """Save the cleaned dataframe to a CSV file."""
    CLEANED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(CLEANED_OUTPUT_PATH)
    print(f"Cleaned dataframe saved to: {CLEANED_OUTPUT_PATH}")
