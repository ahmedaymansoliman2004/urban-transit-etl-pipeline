from __future__ import annotations

import logging

from src.ingestion import OUTPUT_FILE, run_ingestion
from src.schema_validation import (
    SCHEMA_REPORT_PATH,
    save_validation_report,
    validate_schema,
)

logger = logging.getLogger(__name__)


def run_ingestion_validation():
    """
    Run the ingestion stage first, then validate the resulting dataframe schema.
    """
    logger.info("Starting Ingestion + Schema Validation...")

    files_list, dataframe = run_ingestion()

    validation_report = validate_schema(dataframe)
    validation_report["files_read"] = files_list
    validation_report["ingested_file"] = str(OUTPUT_FILE)

    save_validation_report(validation_report, SCHEMA_REPORT_PATH)

    if not validation_report["schema_valid"]:
        logger.error(
            f"Schema validation failed. Check report: {SCHEMA_REPORT_PATH}"
        )
        raise ValueError(
            f"Schema validation failed. Check report: {SCHEMA_REPORT_PATH}"
        )

    logger.info("Ingestion + Schema Validation completed successfully.")
    logger.info(f"Files read: {len(files_list)}")
    logger.info(f"Rows: {validation_report['row_count']}")
    logger.info(f"Columns: {validation_report['column_count']}")
    logger.info(f"Report saved to: {SCHEMA_REPORT_PATH}")

    return "Ingestion Validation Completed Successfully"


if __name__ == "__main__":
    run_ingestion_validation()