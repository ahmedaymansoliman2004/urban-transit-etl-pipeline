import os
from dotenv import load_dotenv

# Resolve base directory dynamically
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
ENV_PATH = os.path.join(BASE_DIR, "config", ".env")
load_dotenv(ENV_PATH)

# Google Cloud configuration
GCP_PROJECT_ID = "urban-transit-sandbox-depi"
GCP_DATASET = "transit_data"
GCP_PROCESSED_TABLE = "processed_tripdata"

# Directory configurations
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")