## PRIMA‑BI‑METADATA

PRIMA‑BI‑METADATA is a lightweight ETL pipeline that ingests BI metadata, normalizes it into a structured internal model, and persists it into a local SQLite database using an idempotent upsert strategy. The project follows a modular architecture inspired by the original design document and supports incremental, repeatable data refreshes.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture and Responsibilities](#architecture-and-responsibilities)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [What Has Been Implemented](#what-has-been-implemented)
- [Running the Pipeline](#running-the-pipeline)
- [Running Tests](#running-tests)
- [Next Steps](#next-steps)

---

### Project Overview

The goal of this project is to maintain an up‑to‑date local metadata store by fetching BI metadata from an external source, transforming it into a clean and consistent internal representation, and writing it into a SQLite database. The system is designed to be simple, reliable, and easy to extend.

The pipeline consists of three main phases:

- **Fetch** — Retrieve metadata from a JSON endpoint or API.  
- **Transform** — Normalize and validate the raw data.  
- **Upsert** — Insert or update rows in the `metadata` table using conflict resolution.

---

### Architecture and Responsibilities

The repository is organized into clear, single‑responsibility modules:

prima-bi-metadata/
│
├── src/prima_bi_metadata/
│   ├── config.py
│   ├── main.py
│   ├── transform.py
│   ├── storage.py
│   └── init.py
│
├── data/
│   └── sample_data.json
│
├── metadata.db
├── tests/
├── README.md
└── pyproject.toml



#### Module descriptions

- **`config.py`** — Centralizes configuration such as API base URL and logging level.  
- **`main.py`** — Orchestrates the ETL workflow: fetch → transform → upsert.  
- **`transform.py`** — Converts raw JSON into a normalized DataFrame, handling missing values and type coercion.  
- **`storage.py`** — Performs safe, idempotent upserts into SQLite, including timestamp serialization and integer normalization.  
- **`metadata.db`** — Local SQLite database storing the `metadata` table.  
- **`tests/`** — Contains schema and upsert tests.  
- **Utility scripts** (`show_schema.py`, `check_db.py`) — Used during development for database inspection.

---

### Data Flow

#### 1. Fetch
`main.py` retrieves metadata from a configured endpoint.  
The system logs the request, response status, and number of records fetched.

#### 2. Transform
`transform.py` processes the raw payload into a clean DataFrame:
- consistent field names  
- conversion of missing values  
- integer coercion  
- timestamp normalization  
- validation of required fields  

#### 3. Upsert
`storage.py` writes the transformed data into SQLite using a bulk upsert:
- `NaN` → `None`  
- timestamps → ISO 8601 strings  
- integers coerced safely  
- `ON CONFLICT(asset_id) DO UPDATE` ensures idempotency  
- safe transaction handling  
- logs number of inserted/updated rows  

---

### Database Schema

The `metadata` table is defined as follows:

```sql
CREATE TABLE metadata (
    asset_id VARCHAR NOT NULL,
    asset_name VARCHAR,
    owner VARCHAR,
    last_updated DATETIME,
    last_viewed DATETIME,
    views_last_30d INTEGER,
    last_refresh DATETIME,
    refresh_status VARCHAR,
    status VARCHAR,
    last_synced_at DATETIME,
    PRIMARY KEY (asset_id)
);


### What Has Been Implemented

The following components have been fully developed based on the original design document and the work completed during implementation:

- **End‑to‑end ETL pipeline** including fetch, transform, and upsert stages.
- **Complete rewrite of the storage layer**, ensuring:
  - correct handling of missing values (`NaN → None`)
  - safe serialization of timestamps into ISO 8601 strings
  - integer coercion for numeric fields
  - SQLAlchemy‑compatible bulk execution
  - idempotent updates using `ON CONFLICT`
- **Detailed logging** across all stages for easier debugging and observability.
- **Validation of the database schema** using development scripts (`show_schema.py`, `check_db.py`).
- **Successful execution of the pipeline** using sample data, confirming correct insertion and update behavior.
- **Initial test suite** including:
  - schema validation tests
  - upsert behavior tests using a temporary SQLite database
- **Environment setup improvements**, including:
  - identifying Python version constraints in `pyproject.toml`
  - running tests in an isolated virtual environment

---

### Running the Pipeline

You can run the full ETL pipeline using Poetry:

bash
poetry run python -m prima_bi_metadata.main

Or using a standalone Python environment:
bash

python -m prima_bi_metadata.main

Running Tests

To run the test suite in an isolated environment:
bash

python -m venv .venv-test
.\.venv-test\Scripts\Activate.ps1
pip install -e .
pip install pytest sqlalchemy pandas
pytest -q

This setup ensures that all dependencies required for testing are available without modifying the main project environment.
