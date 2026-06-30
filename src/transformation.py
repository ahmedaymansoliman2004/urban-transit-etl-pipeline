from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json
from typing import Any

import polars as pl

# File paths
TRANSFORMATION_REPORT_PATH = Path("logs/transformation_report.json")
TRANSFORMED_OUTPUT_PATH = Path("data/interim/transformed_tripdata.csv")


def create_transformation_report(initial_rows: int) -> dict[str, Any]:
    """Initialize transformation metrics."""
    return {
        "transformation_time": datetime.now().isoformat(timespec="seconds"),
        "initial_rows": initial_rows,
        "final_rows": initial_rows,
        "removed_rows": 0,
        "features_engineered": [],
    }


def transform_data(df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Apply business rules and feature engineering."""
    report = create_transformation_report(df.height)

    # Reconstruct datetime
    df = df.with_columns([
        (pl.col("start_date").cast(pl.Utf8) + " " + pl.col("start_time"))
        .str.strptime(pl.Datetime, "%Y-%m-%d %I:%M:%S %p")
        .alias("start_datetime"),
        
        (pl.col("end_date").cast(pl.Utf8) + " " + pl.col("end_time"))
        .str.strptime(pl.Datetime, "%Y-%m-%d %I:%M:%S %p")
        .alias("end_datetime")
    ])

    # Calculate duration
    df = df.with_columns(
        ((pl.col("end_datetime") - pl.col("start_datetime")).dt.total_seconds() / 60)
        .round(2)
        .alias("trip_duration_minutes")
    )
    report["features_engineered"].append("trip_duration_minutes")

    # Filter invalid records
    before_filter = df.height
    df = df.filter(pl.col("trip_duration_minutes") > 0)
    report["removed_rows"] += (before_filter - df.height)

    # Extract time features
    df = df.with_columns([
        pl.col("start_datetime").dt.hour().alias("pickup_hour"),
        pl.col("start_datetime").dt.strftime("%A").alias("day_of_week"),
        pl.col("start_datetime").dt.strftime("%B").alias("month_name")
    ])
    report["features_engineered"].extend(["pickup_hour", "day_of_week", "month_name"])

    # Apply business rules
    df = df.with_columns([
        pl.when(pl.col("trip_duration_minutes") < 10).then(pl.lit("Short Trip"))
        .when((pl.col("trip_duration_minutes") >= 10) & (pl.col("trip_duration_minutes") <= 30)).then(pl.lit("Medium Trip"))
        .otherwise(pl.lit("Long Trip"))
        .alias("trip_category"),
        
        pl.when(pl.col("day_of_week").is_in(["Friday", "Saturday"])).then(pl.lit("Weekend"))
        .otherwise(pl.lit("Weekday"))
        .alias("day_type")
    ])
    report["features_engineered"].extend(["trip_category", "day_type"])

    # Clean up temp columns
    df = df.drop(["start_datetime", "end_datetime"])
    
    report["final_rows"] = df.height

    return df, report


def save_transformation_report(report: dict[str, Any]) -> None:
    """Save report to JSON."""
    TRANSFORMATION_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TRANSFORMATION_REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4, ensure_ascii=False)
    print(f"Transformation report saved to: {TRANSFORMATION_REPORT_PATH}")


def save_transformed_dataframe(df: pl.DataFrame) -> None:
    """Save dataframe to CSV."""
    TRANSFORMED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(TRANSFORMED_OUTPUT_PATH)
    print(f"Transformed dataframe saved to: {TRANSFORMED_OUTPUT_PATH}")