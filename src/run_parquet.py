from src.parquet_export import main


def run_parquet_pipeline():
    """
    Run the Parquet Export stage.
    """
    main()
    return "Parquet Export Completed Successfully"


if __name__ == "__main__":
    run_parquet_pipeline()
