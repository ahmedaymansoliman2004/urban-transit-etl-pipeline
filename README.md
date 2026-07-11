# 🚇 Urban Transit ETL Pipeline

**Enterprise-Grade Batch Data Pipeline for Large-Scale Urban Mobility Data**

*A Data Engineering Graduation Project developed for the Digital Egypt Pioneers Initiative (DEPI).*

---

## 📑 Table of Contents

* [Overview](#-overview)
* [Features](#-features)
* [Objectives](#-objectives)
* [Architecture](#-architecture)
  * [Architecture Diagram](#architecture-diagram)
  * [ETL Workflow](#etl-workflow)
* [Folder Structure](#-folder-structure)
* [Technologies](#-technologies)
* [Dataset](#-dataset)
  * [Dataset Statistics](#dataset-statistics)
  * [Data Profile](#data-profile)
* [Installation](#-installation)
  * [Requirements](#requirements)
  * [Quick Start](#quick-start)
* [Usage](#-usage)
  * [Configuration](#configuration)
* [Pipeline Modules](#-pipeline-modules)
* [BigQuery Integration](#-bigquery-integration)
* [SQL Analytics](#-sql-analytics)
* [Automation & Orchestration](#-automation--orchestration)
* [Logging & Monitoring](#-logging--monitoring)
* [Documentation](#-documentation)
* [Roadmap](#-roadmap)
* [Team](#-team)
* [Future Improvements](#-future-improvements)
* [Contributing](#-contributing)
* [License](#-license)
* [Acknowledgements](#-acknowledgements)

---

## 📖 Overview

The **Urban Transit ETL Pipeline** is a comprehensive, production-style batch ETL (Extract, Transform, Load) system designed to process large-scale urban transit open data. Developed as a capstone graduation project for the **Digital Egypt Pioneers Initiative (DEPI)**, this pipeline demonstrates end-to-end data engineering best practices.

The system programmatically ingests raw trip-level CSV files, performs rigorous schema validation, applies complex data cleaning and transformation logic using modern dataframe libraries (Polars/Pandas), stores intermediate artifacts in columnar formats (Apache Parquet), and securely loads the refined data into a cloud data warehouse (Google BigQuery) for analytical querying.

> [!NOTE]
> This project is designed as an educational and practical portfolio piece, built to emulate the rigor, structure, and fault-tolerance of an enterprise Data Engineering environment.

---

## ✨ Features

* **High-Performance Processing:** Utilizes `Polars` for multi-threaded, memory-efficient data transformations.
* **Strict Schema Enforcement:** Validates incoming data against pre-defined schemas to prevent pipeline contamination.
* **Resilient Data Cleaning:** Programmatic handling of missing coordinates, orphaned station IDs, and anomalous temporal data.
* **Columnar Storage Optimization:** Converts processed datasets into compressed Apache Parquet formats, reducing storage footprints and accelerating downstream reads.
* **Cloud Data Warehouse Integration:** Automated schema-mapped loading into Google BigQuery.
* **Comprehensive Analytics:** Includes pre-built SQL routines for mobility trend analysis (e.g., peak hour calculations, geographic route mapping).
* **Robust Logging & Telemetry:** JSON-formatted logs with granular trace levels for every ETL stage.
* **Orchestration Ready:** Structured to be triggered via `Cron` or seamlessly integrated into an `Apache Airflow` DAG.

---

## 🎯 Objectives

1. **Demonstrate Full Lifecycle Data Engineering:** Build an end-to-end pipeline from raw data extraction to actionable cloud-based analytics.
2. **Handle Real-World Data Anomalies:** Successfully process over 1.8 million records containing null values, missing geographical points, and diverse data types.
3. **Implement Enterprise Best Practices:** Ensure modular code architecture, strict environment separation, configuration-driven execution, and comprehensive documentation.
4. **Optimize for Scale:** Prove the viability of modern tools like Polars and Parquet over legacy row-based processing techniques.

---

## 🏗️ Architecture

The architecture follows a classic Batch ETL pattern, strictly decoupling extraction, processing, and loading to ensure modularity and ease of debugging.

### Architecture Diagram

```text
+-------------------+       +-------------------+       +-------------------+
|   Source System   |       |   Ingestion Zone  |       | Processing Engine |
| (Citi Bike Open   | ----> |  (Local Storage/  | ----> | (Python / Polars) |
|  Data API / S3)   |       |   Cloud Bucket)   |       |                   |
+-------------------+       +-------------------+       +-------------------+
                                                                  |
                                                                  v
+-------------------+       +-------------------+       +-------------------+
|   BI Dashboards   |       | Data Warehouse    |       | Storage Artifacts |
|  (Looker / Tableau| <---- | (Google BigQuery) | <---- | (Apache Parquet)  |
|    Reporting)     |       |                   |       |                   |
+-------------------+       +-------------------+       +-------------------+


```

### ETL Workflow

The detailed step-by-step data journey through the pipeline:

```text
[Raw CSV] 
   │
   ├─► 1. Data Ingestion (Chunked reading, file discovery)
   │
   ├─► 2. Schema Validation (Type checking, required fields)
   │
   ├─► 3. Data Cleaning (Handling NULLs, deduplication checks)
   │
   ├─► 4. Data Transformation (Date parsing, geo-spatial derivations, metric calculation)
   │
   ├─► 5. Post Validation (Checking transformation integrity)
   │
   ├─► 6. Parquet Conversion (Snappy compression, partitioning)
   │
   ├─► 7. BigQuery Load (Client API, append/overwrite strategies)
   │
   ├─► 8. SQL Analytics (Aggregations, materialized views creation)
   │
   └─► [Analytics Ready]


```

---

## 📁 Folder Structure

The repository enforces a strict, modular directory tree typical of professional Python-based data engineering projects.

```text
urban-transit-etl-pipeline/
├── .github/                  # CI/CD workflows and issue templates
├── config/                   # YAML/JSON configuration files for the pipeline
├── data/                     
│   ├── raw/                  # Immutable original CSV files (git-ignored)
│   ├── processed/            # Intermediate processed flat files
│   ├── parquet/              # Final compressed Parquet output
│   └── sample/               # Small subset of data for testing/CI
├── docs/                     # Project documentation
│   ├── diagrams/             # Architecture and ERD diagrams
│   ├── reports/              # Profiling and analytics reports
│   ├── screenshots/          # Visual assets
│   └── presentation/         # Final project presentation decks
├── logs/                     # JSON and text application logs
├── notebooks/                # Jupyter notebooks for EDA and prototyping
├── sql/                      # DDL and DML SQL scripts for BigQuery
├── src/                      # Core pipeline source code
│   ├── ingestion/            # Extraction and loading modules
│   ├── validation/           # Schema enforcement scripts
│   ├── cleaning/             # Null handling and deduplication
│   ├── transformation/       # Business logic and calculated fields
│   ├── post_validation/      # Final quality assurance checks
│   ├── parquet/              # Arrow/Parquet conversion utilities
│   ├── bigquery/             # GCP client interaction
│   ├── analytics/            # Python wrappers for analytical queries
│   ├── monitoring/           # Data quality telemetry
│   ├── scheduler/            # Cron scripts or Airflow DAGs
│   └── utils/                # Helper functions (logging, config parsing)
├── tests/                    # Pytest unit and integration tests
├── README.md                 # Project root documentation
├── requirements.txt          # Python dependency specifications
├── .gitignore                # Untracked files configuration
├── LICENSE                   # Open-source license definition
└── CHANGELOG.md              # Version history and release notes


```

---

## 💻 Technologies

The pipeline is built on a modern, robust, and open-source data stack:

| Category | Technology | Purpose |
| --- | --- | --- |
| **Programming** | Python 3.10+ | Core pipeline logic, scripting, API integrations. |
| **Data Processing** | Polars | High-speed, multi-threaded DataFrame operations and transformations. |
| **Data Manipulation** | Pandas | Fallback and legacy data manipulation; EDA support. |
| **Storage Format** | Apache Parquet | Columnar data storage, schema evolution, and heavy compression. |
| **Data Warehouse** | Google BigQuery | Scalable, serverless enterprise cloud data warehousing. |
| **Cloud Provider** | Google Cloud (GCP) | Identity Access Management (IAM) and Cloud Storage. |
| **Query Language** | SQL (Standard) | Aggregations, analytics, and materialized views in BigQuery. |
| **Orchestration** | Apache Airflow / Cron | Pipeline scheduling, dependency management, and workflow automation. |
| **Serialization** | PyArrow / JSON | Arrow memory format for Parquet; JSON for structured logging. |
| **Version Control** | Git & GitHub | Source code management, team collaboration, and CI/CD. |

---

## 📊 Dataset

The project relies on real-world, large-scale urban transit open data from the NYC Citi Bike system.

* **Dataset Name:** NYC Citi Bike Trip Data – January 2024
* **Dataset Source:** [Citi Bike System Data](https://citibikenyc.com/system-data)
* **Raw ZIP Archive:** `202401-citibike-tripdata.zip`
* **Extracted CSV Files:**
* `202401-citibike-tripdata_1.csv`
* `202401-citibike-tripdata_2.csv`



> [!WARNING]
> Due to GitHub file size limits, the raw `.csv` and `.zip` files are **not** included in this repository. You must download them via the script in the `src/ingestion` directory or manually from the source and place them into `data/raw/`.

### Dataset Statistics

Initial profiling guarantees the scale and cleanliness of the data prior to deep transformation.

| Metric | Value |
| --- | --- |
| **Number of CSV files** | 2 |
| **Number of Rows** | 1,888,085 |
| **Number of Columns** | 13 |
| **Duplicate Rows** | 0 |
| **Duplicate Ride IDs** | 0 |

### Data Profile

The dataset contains the following features: `ride_id`, `rideable_type`, `started_at`, `ended_at`, `start_station_name`, `start_station_id`, `end_station_name`, `end_station_id`, `start_lat`, `start_lng`, `end_lat`, `end_lng`, `member_casual`.

#### Missing Values Summary

One of the primary tasks of the data cleaning module is to handle the following null values strategically:

| Column Name | Missing Count | Missing Percentage |
| --- | --- | --- |
| `ride_id` | 0 | 0.00% |
| `rideable_type` | 0 | 0.00% |
| `started_at` | 0 | 0.00% |
| `ended_at` | 0 | 0.00% |
| `start_station_name` | 1,160 | 0.06% |
| `start_station_id` | 1,160 | 0.06% |
| `end_station_name` | 5,505 | 0.29% |
| `end_station_id` | 5,505 | 0.29% |
| `start_lat` | 1,160 | 0.06% |
| `start_lng` | 1,160 | 0.06% |
| `end_lat` | 5,486 | 0.29% |
| `end_lng` | 5,486 | 0.29% |
| `member_casual` | 0 | 0.00% |

#### Date Range

The dataset strictly bounds to January 2024, with some late-night overlap from NYE 2023.

| Field | Minimum | Maximum |
| --- | --- | --- |
| `started_at` | 2023-12-31 02:36:55.648 | 2024-01-31 23:58:30.270 |
| `ended_at` | 2024-01-01 00:00:08.272 | 2024-01-31 23:59:56.370 |

---

## 🛠️ Installation

Follow these steps to set up the environment locally.

### Requirements

* Python 3.10 or higher
* Git
* A Google Cloud Platform (GCP) account with BigQuery enabled
* A Google Service Account JSON key (with BigQuery Data Editor roles)

### Quick Start

1. **Clone the repository:**

```bash
git clone [https://github.com/your-username/urban-transit-etl-pipeline.git](https://github.com/your-username/urban-transit-etl-pipeline.git)
cd urban-transit-etl-pipeline

```

2. **Create a virtual environment:**

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

```

3. **Install dependencies:**

```bash
pip install --upgrade pip
pip install -r requirements.txt

```

4. **Prepare Data Directories:**
The required `.gitkeep` directories are already present, but ensure the structure exists:

```bash
mkdir -p data/raw data/processed data/parquet logs config

```

5. **Download the Dataset:**
Place `202401-citibike-tripdata_1.csv` and `202401-citibike-tripdata_2.csv` into `data/raw/`.

---

## 🚀 Usage

The pipeline can be executed entirely via the command line or run modularly.

### Configuration

Before running the pipeline, set up your configuration. Copy the template configuration file:

```bash
cp config/config.template.yaml config/config.yaml

```

Edit `config/config.yaml` to include your GCP project details:

```yaml
gcp:
  project_id: "your-gcp-project-id"
  dataset_id: "urban_transit_dw"
  table_id: "citi_bike_trips_202401"
  credentials_path: "/path/to/your/service-account.json"

pipeline:
  raw_data_dir: "data/raw"
  parquet_dir: "data/parquet"
  batch_size: 100000

```

### Running the Pipeline

Execute the master orchestrator script:

```bash
python src/main.py --config config/config.yaml

```

To run individual modules (useful for debugging):

```bash
# Run only validation and cleaning
python src/cleaning/clean_data.py --input data/raw --output data/processed

# Run Parquet conversion
python src/parquet/convert.py --input data/processed --output data/parquet

```

---

## 🧩 Pipeline Modules

A deep dive into the engineering logic behind each pipeline stage.

### 1. Data Ingestion

Reads multiple large CSV files efficiently. Instead of loading the entire 1.8M rows into RAM simultaneously, the ingestion module utilizes `polars.scan_csv()` for lazy evaluation, generating an optimized query plan before execution.

### 2. Schema Validation

Checks the structure of the incoming data against a defined schema contract.

* Validates that `ride_id` is a string of expected length.
* Ensures `started_at` and `ended_at` can be cast to `datetime`.
* Rejects or quarantines files that add unexpected columns or change delimiters.

### 3. Data Cleaning

Addresses the missing values identified in the profiling stage:

* **Geo-Coordinates:** Missing `end_lat` and `end_lng` (5,486 rows) are isolated. Since geographical tracking is critical, these rows are flagged and stored in a separate `data_quality_issues` table rather than dropped, preserving the trip duration metric.
* **Station IDs:** Standardizes formatting inconsistencies in `start_station_id` (1,160 missing).
* **Deduplication:** Confirms 0 duplicate `ride_id`s, ensuring transactional integrity.

### 4. Data Transformation

Applies complex business logic to prepare the data for analytics:

* **Derived Columns:** Calculates `trip_duration_minutes` (Difference between `ended_at` and `started_at`).
* **Temporal Features:** Extracts `day_of_week`, `hour_of_day`, and `is_weekend` to support peak-hour analytics.
* **Distance Calculation:** Applies the Haversine formula on latitude/longitude columns to estimate `straight_line_distance_km`.

### 5. Post Validation

Runs statistical assertions post-transformation:

* Asserts `trip_duration_minutes` > 0.
* Asserts `started_at` <= `ended_at`.

### 6. Apache Parquet Conversion

Converts the memory representation into an Apache Parquet format using PyArrow.

* **Partitioning:** Data is partitioned by `started_at` (Date).
* **Compression:** Snappy compression is applied, reducing the file size by approximately 75% compared to raw CSVs while drastically speeding up BigQuery ingestion.

---

## ☁️ BigQuery Integration

The processed Parquet files are loaded into Google BigQuery using the `google-cloud-bigquery` library.

**Load Strategy Strategy:** `WRITE_TRUNCATE` (for idempotency during backfills) or `WRITE_APPEND` (for daily batching).

**Schema Auto-Detection:** The pipeline leverages Parquet's strict schema definitions to automatically create and map BigQuery table structures without manual DDL intervention.

---

## 📈 SQL Analytics

Once the data is warehoused in BigQuery, analytical views are created to serve Business Intelligence dashboards.

```sql
-- sql/analytics/peak_hours.sql
SELECT 
    EXTRACT(HOUR FROM started_at) AS hour_of_day,
    member_casual,
    COUNT(ride_id) AS total_rides,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes
FROM 
    `your-project.urban_transit_dw.citi_bike_trips_202401`
GROUP BY 
    hour_of_day, member_casual
ORDER BY 
    total_rides DESC;

```

```sql
-- sql/analytics/popular_routes.sql
SELECT 
    start_station_name,
    end_station_name,
    COUNT(ride_id) AS route_volume
FROM 
    `your-project.urban_transit_dw.citi_bike_trips_202401`
WHERE 
    start_station_name IS NOT NULL 
    AND end_station_name IS NOT NULL
GROUP BY 
    start_station_name, end_station_name
ORDER BY 
    route_volume DESC
LIMIT 10;

```

---

## ⚙️ Automation & Orchestration

To transform this from a simple script to an enterprise pipeline, execution is automated.

* **Cron:** For lightweight environments, `src/scheduler/cron_job.sh` wraps the pipeline in a shell script that can be executed nightly.
* **Apache Airflow:** For full-scale deployments, the `src/scheduler/transit_dag.py` defines a Directed Acyclic Graph (DAG) handling retries, dependency mapping, and failure alerts via email.

---

## 📝 Logging & Monitoring

### Logging

The `src/utils/logger.py` module outputs machine-readable JSON logs for ingestion into systems like ELK or Datadog, alongside human-readable console outputs.

```json
{
  "timestamp": "2026-06-29T03:14:55.000Z",
  "level": "INFO",
  "module": "transformation",
  "message": "Successfully calculated trip_duration_minutes for 1,888,085 rows.",
  "duration_ms": 1420
}

```

### Monitoring

Post-validation scripts act as data quality monitors. If missing values exceed a configurable threshold (e.g., > 1% of the daily batch), the pipeline throws a `DataQualityException` and halts BigQuery insertion.

---

## 📚 Documentation

Detailed documentation is essential for hand-offs and maintenance. Check the `/docs` folder for:

* `docs/dataset_source.md` - Context on Citi Bike APIs.
* `docs/data_dictionary.md` - Definition of every column, data type, and unit of measurement.
* `docs/initial_data_profile.md` - The raw Jupyter Notebook exports containing the EDA.

---

## 🗺️ Roadmap

| Phase | Task | Status |
| --- | --- | --- |
| **Phase 1** | Project Scoping & Dataset Identification | ✅ |
| **Phase 2** | Raw Data Collection & EDA Profiling | ✅ |
| **Phase 3** | Ingestion & Schema Validation Modules | ✅ |
| **Phase 4** | Polars Data Cleaning & Transformation | ✅ |
| **Phase 5** | Parquet Conversion & BigQuery Loading | ✅ |
| **Phase 6** | SQL Analytics & Materialized Views | ✅ |
| **Phase 7** | Automation (Cron/Airflow) & Logging | ✅ |
| **Phase 8** | Final Presentation, Demo, & Polish | ✅ |

**Final Project Deadline:** 17 July 2026

---

## 👥 Team

This project was developed by the DEPI Data Engineering Team.

| Name | Role | GitHub |
| --- | --- | --- |
| **Ahmed Ayman Soliman** | Project Manager & Data Engineer | [@ahmedaymansoliman2004](https://github.com/ahmedaymansoliman2004) |
| **May Mohammed Massoud** | Data Engineer | [@Maimohamed2](https://github.com/Maimohamed2) |
| **Yousef Loley Abdelrahman** | Data Engineer | [@youssefloly](https://github.com/youssefloly) |

---

## 🔮 Future Improvements

While this pipeline fulfills graduation requirements, enterprise pipelines are constantly evolving. Future iterations could include:

1. **Streaming Data Ingestion:** Replacing batch CSV drops with real-time Kafka streams tapping directly into the GBFS (General Bikeshare Feed Specification) JSON APIs.
2. **dbt Integration:** Migrating the SQL transformation layer from raw BigQuery SQL scripts to `dbt` (Data Build Tool) for better lineage and testing.
3. **Terraform/IaC:** Managing the GCP infrastructure (Storage Buckets, BigQuery Datasets, Service Accounts) using Infrastructure as Code.
4. **Geospatial Analytics Hub:** Connecting BigQuery directly to a tool like Kepler.gl for dynamic route visualization.

---

## 🤝 Contributing

As an educational open-source project, contributions, suggestions, and feedback are welcome.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.

---

## 🎓 Acknowledgements

* [Digital Egypt Pioneers Initiative (DEPI) / Roa'd Masr El Rakameya](https://www.google.com/search?q=https://depi.gov.eg/) for the comprehensive training and opportunity.
* [NYC Citi Bike](https://citibikenyc.com/) for their commitment to Open Data and providing the foundational dataset.
* [Polars Community](https://pola.rs/) for building an exceptionally fast dataframe library.

```
