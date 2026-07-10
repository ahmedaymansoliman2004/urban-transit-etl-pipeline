from __future__ import annotations

from pathlib import Path
from datetime import datetime
import hashlib
import json
import os
import socket
from typing import Any

import polars as pl

# Resolve project root from this file so paths work locally and in Docker
PROJECT_DIR = Path(__file__).resolve().parent.parent
TRANSFORMATION_REPORT_PATH = PROJECT_DIR / "logs" / "transformation_report.json"
TRANSFORMED_OUTPUT_PATH = PROJECT_DIR / "data" / "interim" / "transformed_tripdata.csv"
TRANSFORMED_MANIFEST_PATH = PROJECT_DIR / "data" / "interim" / "transformed_tripdata.manifest.json"


def _hostname() -> str:
    return socket.gethostname()


def log_file_fingerprint(path: Path, label: str) -> None:
    """Print size/mtime/md5/container-host for a file so cross-container
    staleness (different bind mount, baked-in image copy, etc.) is
    immediately visible in the task logs instead of silently corrupting
    downstream row counts."""
    stat = path.stat()
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    print(
        f"[FINGERPRINT:{label}] host={_hostname()} path={path} "
        f"size={stat.st_size} mtime={stat.st_mtime} md5={hasher.hexdigest()}"
    )


def create_transformation_report(initial_rows: int) -> dict[str, Any]:
    return {
        "transformation_time": datetime.now().isoformat(timespec="seconds"),
        "initial_rows": initial_rows,
        "final_rows": initial_rows,
        "removed_rows": 0,
        "features_engineered": [],
    }


def transform_data(df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    report = create_transformation_report(df.height)

    # Reconstruct datetime (Added strip_chars to remove hidden spaces)
    df = df.with_columns([
        (pl.col("start_date").cast(pl.Utf8).str.strip_chars() + " " + pl.col("start_time").cast(pl.Utf8).str.strip_chars())
        .str.strptime(pl.Datetime, "%Y-%m-%d %I:%M:%S %p", strict=False)
        .alias("start_datetime"),
        
        (pl.col("end_date").cast(pl.Utf8).str.strip_chars() + " " + pl.col("end_time").cast(pl.Utf8).str.strip_chars())
        .str.strptime(pl.Datetime, "%Y-%m-%d %I:%M:%S %p", strict=False)
        .alias("end_datetime")
    ])

    # Calculate duration
    df = df.with_columns(
        ((pl.col("end_datetime") - pl.col("start_datetime")).dt.total_seconds() / 60)
        .round(2)
        .alias("trip_duration_minutes")
    )
    report["features_engineered"].append("trip_duration_minutes")

    # ==========================================
    # 🚨 SUPER DIAGNOSTIC BLOCK (Catching NULLs)
    # ==========================================
    # Now checking for both <= 0 AND is_null()
    invalid = df.filter(pl.col("trip_duration_minutes").is_null() | (pl.col("trip_duration_minutes") <= 0))
    
    if invalid.height > 0:
        invalid_rate = invalid.height / df.height
        print(f"\n[🚨 CRITICAL ALERT] {invalid.height} rows ({invalid_rate:.1%}) have NULL or invalid duration!")
        
        if "source_file" in df.columns:
            print("\n--- Breakdown by Source File ---")
            print(invalid.group_by("source_file").len().sort("len", descending=True))
            
            print("\n--- EXACT Data Causing the Crash ---")
            print(invalid.select([
                "source_file", 
                "start_date", 
                "start_time", 
                "end_date", 
                "end_time", 
                "trip_duration_minutes"
            ]).head(10))
            print("==========================================\n")
    # ==========================================

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
    TRANSFORMATION_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TRANSFORMATION_REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4, ensure_ascii=False)
    print(f"Transformation report saved to: {TRANSFORMATION_REPORT_PATH}")


def save_transformed_dataframe(df: pl.DataFrame) -> None:
    TRANSFORMED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Write to a temp file in the SAME directory, then atomically rename.
    # This guarantees any reader (even one on a different container that
    # shares the bind mount) only ever sees a complete, fully-flushed file
    # -- never a half-written one -- and os.replace() is atomic on POSIX
    # filesystems, including the ext4 filesystem WSL2/Docker Desktop uses
    # for non-/mnt/c paths.
    tmp_path = TRANSFORMED_OUTPUT_PATH.with_suffix(".csv.tmp")
    df.write_csv(tmp_path)
    os.replace(tmp_path, TRANSFORMED_OUTPUT_PATH)

    print(f"Transformed dataframe saved to: {TRANSFORMED_OUTPUT_PATH}")
    log_file_fingerprint(TRANSFORMED_OUTPUT_PATH, "transformation:write")

    # Row-count manifest: the contract the next task (parquet_export) will
    # verify against before it trusts what it reads off disk.
    manifest = {
        "row_count": df.height,
        "column_count": df.width,
        "source_path": str(TRANSFORMED_OUTPUT_PATH),
        "written_at": datetime.now().isoformat(timespec="seconds"),
        "written_by_host": _hostname(),
        "md5": hashlib.md5(TRANSFORMED_OUTPUT_PATH.read_bytes()).hexdigest(),
    }
    TRANSFORMED_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    manifest_tmp = TRANSFORMED_MANIFEST_PATH.with_suffix(".json.tmp")
    with open(manifest_tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)
    os.replace(manifest_tmp, TRANSFORMED_MANIFEST_PATH)
    print(f"Transformation manifest saved to: {TRANSFORMED_MANIFEST_PATH}")