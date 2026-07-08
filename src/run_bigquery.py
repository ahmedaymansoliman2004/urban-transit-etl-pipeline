from src.bigquery_loader import main


def run_bigquery_pipeline():
    """
    Run the BigQuery loading stage.
    """
    main()
    return "BigQuery Load Completed Successfully"


if __name__ == "__main__":
    run_bigquery_pipeline()
