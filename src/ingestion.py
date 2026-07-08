from pathlib import Path
import zipfile
import polars as pl

# Project root داخل Docker
PROJECT_DIR = Path("/opt/airflow/project")

RAW_DIR = PROJECT_DIR / "data" / "raw"
EXTRACT_DIR = PROJECT_DIR / "data" / "interim" / "extracted"
OUTPUT_DIR = PROJECT_DIR / "data" / "interim"
OUTPUT_FILE = OUTPUT_DIR / "ingested_tripdata.csv"

REQUIRED_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]

SCHEMA_OVERRIDES = {
    "ride_id": pl.Utf8,
    "rideable_type": pl.Utf8,
    "started_at": pl.Utf8,
    "ended_at": pl.Utf8,
    "start_station_name": pl.Utf8,
    "start_station_id": pl.Utf8,
    "end_station_name": pl.Utf8,
    "end_station_id": pl.Utf8,
    "start_lat": pl.Float64,
    "start_lng": pl.Float64,
    "end_lat": pl.Float64,
    "end_lng": pl.Float64,
    "member_casual": pl.Utf8,
}


def get_csv_files(raw_dir: Path = RAW_DIR, extract_dir: Path = EXTRACT_DIR) -> list[Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    raw_csv_files = sorted(raw_dir.glob("*.csv"))
    raw_csv_files = [file for file in raw_csv_files if file.name != OUTPUT_FILE.name]

    if raw_csv_files:
        return raw_csv_files

    zip_files = sorted(raw_dir.glob("*.zip"))
    if not zip_files:
        raise FileNotFoundError("No CSV or ZIP files found in data/raw/.")

    for zip_file in zip_files:
        print(f"Extracting ZIP file: {zip_file}")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

    extracted_csv_files = sorted(extract_dir.rglob("*.csv"))

    if not extracted_csv_files:
        raise FileNotFoundError("No CSV files found after extracting ZIP files.")

    return extracted_csv_files


def read_and_combine_csv_files(csv_files: list[Path]) -> tuple[list[str], pl.DataFrame]:
    if not csv_files:
        raise ValueError("CSV files list is empty.")

    dataframes = []
    read_files = []

    for file_path in csv_files:
        print(f"Reading file: {file_path.name}")

        df = pl.read_csv(
            file_path,
            schema_overrides=SCHEMA_OVERRIDES,
            try_parse_dates=False,
            null_values=["", "NULL", "null", "None"],
        )

        if df.height == 0:
            raise ValueError(f"CSV file is empty: {file_path}")

        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"File {file_path.name} is missing columns: {missing_columns}"
            )

        df = df.select(REQUIRED_COLUMNS)
        df = df.with_columns(pl.lit(file_path.name).alias("source_file"))

        dataframes.append(df)
        read_files.append(file_path.name)

    combined_df = pl.concat(dataframes, how="vertical")

    if combined_df.height == 0:
        raise ValueError("Combined dataframe is empty.")

    return read_files, combined_df


def split_datetime_columns(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.col("started_at").str.strptime(pl.Datetime, strict=False).alias("started_at_dt"),
            pl.col("ended_at").str.strptime(pl.Datetime, strict=False).alias("ended_at_dt"),
        ]
    )

    invalid_started_at = df.filter(
        pl.col("started_at").is_not_null() &
        pl.col("started_at_dt").is_null()
    ).height

    invalid_ended_at = df.filter(
        pl.col("ended_at").is_not_null() &
        pl.col("ended_at_dt").is_null()
    ).height

    if invalid_started_at > 0 or invalid_ended_at > 0:
        raise ValueError(
            "Invalid datetime values found. "
            f"started_at invalid rows: {invalid_started_at}, "
            f"ended_at invalid rows: {invalid_ended_at}"
        )

    df = df.with_columns(
        [
            pl.col("started_at_dt").dt.date().alias("start_date"),
            pl.col("started_at_dt").dt.strftime("%I:%M:%S %p").alias("start_time"),
            pl.col("ended_at_dt").dt.date().alias("end_date"),
            pl.col("ended_at_dt").dt.strftime("%I:%M:%S %p").alias("end_time"),
        ]
    )

    df = df.drop([
        "started_at",
        "ended_at",
        "started_at_dt",
        "ended_at_dt",
    ])

    final_column_order = [
        "ride_id",
        "rideable_type",
        "start_date",
        "start_time",
        "end_date",
        "end_time",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual",
        "source_file",
    ]

    return df.select(final_column_order)


def save_ingested_dataframe(
    df: pl.DataFrame,
    output_file: Path = OUTPUT_FILE,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_file)
    print(f"Saved ingested dataframe to: {output_file}")


def run_ingestion() -> tuple[list[str], pl.DataFrame]:
    csv_files = get_csv_files()

    files_list, combined_df = read_and_combine_csv_files(csv_files)

    final_df = split_datetime_columns(combined_df)

    # تحرير الذاكرة بعد الانتهاء من استخدام DataFrame القديم
    del combined_df

    save_ingested_dataframe(final_df)

    print("\nData ingestion completed successfully.")
    print(f"Files processed: {files_list}")
    print(f"Total row count: {final_df.height}")
    print(f"Total columns: {len(final_df.columns)}")

    return files_list, final_df


if __name__ == "__main__":
    run_ingestion()