import hashlib
import os
import time
import json
from pathlib import Path
from typing import Any, List, Dict
import polars as pl

# Absolute paths for Docker compatibility
PROJECT_DIR = Path("/opt/airflow/project")
INPUT_CSV_PATH = PROJECT_DIR / "data" / "interim" / "transformed_tripdata.csv"
INPUT_MANIFEST_PATH = PROJECT_DIR / "data" / "interim" / "transformed_tripdata.manifest.json"
OUTPUT_PARQUET_PATH = PROJECT_DIR / "data" / "processed" / "processed_tripdata.parquet"
REPORT_PATH = PROJECT_DIR / "logs" / "parquet_export_report.json"


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
        f"[FINGERPRINT:{label}] host={os.uname().nodename} path={path} "
        f"size={stat.st_size} mtime={stat.st_mtime} md5={hasher.hexdigest()}"
    )


def verify_against_manifest(df: pl.DataFrame, csv_path: Path, manifest_path: Path = INPUT_MANIFEST_PATH) -> None:
    """Fail loudly if the row count we just read doesn't match what the
    transformation task actually wrote. Without this, a stale bind mount
    or an old file baked into the image silently produces a smaller
    (wrong) dataset that flows all the way into BigQuery."""
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Transformation manifest not found: {manifest_path}. "
            "Cannot verify that this container is reading the current "
            "transformed_tripdata.csv -- refusing to proceed blind."
        )

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    expected_rows = manifest["row_count"]
    actual_md5 = hashlib.md5(csv_path.read_bytes()).hexdigest()

    if df.height != expected_rows or actual_md5 != manifest.get("md5"):
        raise ValueError(
            f"Row count or content hash mismatch: manifest expects {expected_rows} rows "
            f"(md5={manifest.get('md5')}), but this container read {df.height} rows "
            f"(md5={actual_md5}) from {csv_path}. This container's filesystem view is "
            "stale relative to what transformation.py wrote — likely a bind-mount cache "
            "coherency lag, not a row-count mismatch."
        )

    print(f"Manifest check passed: {df.height} rows matches transformation output.")

REQUIRED_COLUMNS = [
    "ride_id", 
    "start_station_name", 
    "end_station_name", 
    "trip_duration_minutes"
]

def validate_input_file(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
        
    if file_path.stat().st_size == 0:
        raise ValueError(f"Input file is empty: {file_path}")

def load_dataset(file_path: Path, required_columns: List[str]) -> pl.DataFrame:
    log_file_fingerprint(file_path, "parquet_export:read")

    df = pl.read_csv(
        file_path,
        schema_overrides={
            "start_station_id": pl.Utf8,
            "end_station_id": pl.Utf8
        }
    )

    if df.height == 0:
        raise ValueError("The loaded dataset contains 0 rows.")

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Invalid schema. Missing required columns: {missing_columns}")

    # Hard guard: refuse to export if this container's copy of the CSV
    # doesn't match what the transformation task actually produced.
    verify_against_manifest(df, file_path)   # <-- pass file_path through

    return df

def export_parquet(df: pl.DataFrame, output_path: Path) -> float:
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Export using Snappy compression
        df.write_parquet(output_path, compression="snappy")
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        return round(file_size_mb, 2)
        
    except Exception as e:
        raise IOError(f"Failed to write Parquet file at {output_path}. Error: {e}")

def generate_report(
    status: str, 
    rows: int, 
    columns: int, 
    output_file: str, 
    file_size_mb: float, 
    execution_time: float
) -> Dict[str, Any]:
    
    report = {
        "status": status,
        "rows": rows,
        "columns": columns,
        "compression": "snappy",
        "output_file": output_file,
        "file_size_mb": file_size_mb,
        "execution_time_seconds": round(execution_time, 2)
    }
    
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)
        
    return report

def main() -> Dict[str, Any]:
    start_time = time.time()
    
    try:
        print("Validating input file...")
        validate_input_file(INPUT_CSV_PATH)
        
        print("Reading transformed dataset...")
        df = load_dataset(INPUT_CSV_PATH, REQUIRED_COLUMNS)
        
        print("Exporting to Parquet...")
        file_size_mb = export_parquet(df, OUTPUT_PARQUET_PATH)
        
        execution_time = time.time() - start_time
        
        print("Generating export report...")
        report = generate_report(
            status="success",
            rows=df.height,
            columns=df.width,
            output_file=str(OUTPUT_PARQUET_PATH),
            file_size_mb=file_size_mb,
            execution_time=execution_time
        )
        
        print("\nExport completed successfully!")
        print(f"Rows: {report['rows']}")
        print(f"Size: {report['file_size_mb']} MB")
        print(f"Time: {report['execution_time_seconds']} seconds\n")
        
        return report
        
    except Exception as e:
        execution_time = time.time() - start_time
        print(f"\nExport failed: {e}\n")
        
        generate_report(
            status="failed",
            rows=0,
            columns=0,
            output_file=str(OUTPUT_PARQUET_PATH),
            file_size_mb=0.0,
            execution_time=execution_time
        )
        raise

if __name__ == "__main__":
    main()