import polars as pl

from src.ingestion import run_ingestion
from src.schema_validation import validate_schema
from src.cleaning import (
    clean_data,
    save_cleaning_report,
    save_cleaned_dataframe,
)


def run_cleaning_pipeline() -> pl.DataFrame:
    """
    Execute ingestion, schema validation, and cleaning stages.

    Returns:
        pl.DataFrame: Cleaned dataframe.
    """

    try:
        _, df = run_ingestion()

        schema_report = validate_schema(df)

        if not schema_report.get("schema_valid", False):
            raise ValueError("Schema validation failed.")

        cleaned_df, cleaning_report = clean_data(df)

        save_cleaning_report(cleaning_report)
        save_cleaned_dataframe(cleaned_df)

        print("\n✅ Data Cleaning Completed Successfully")
        print(f"Rows Before: {cleaning_report['initial_rows']:,}")
        print(f"Rows After: {cleaning_report['final_rows']:,}")
        print(f"Rows Removed: {cleaning_report['removed_rows']:,}")

        return "Cleaning completed successfully"

    except Exception as e:
        print(f"\n❌ Cleaning Pipeline Failed: {e}")
        raise


if __name__ == "__main__":
    run_cleaning_pipeline()