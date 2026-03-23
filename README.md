# PRIMA-BI-METADATA

PRIMA-BI-METADATA is a lightweight ETL pipeline that ingests BI metadata from a JSON source, normalizes it into a structured internal model, and persists it into a local SQLite database using an idempotent upsert strategy. The project follows a modular architecture and supports repeatable data refreshes.

---

## Project Overview

The goal of this project is to maintain an up-to-date local metadata store by fetching BI metadata from an external source (served via HTTP), transforming it into a clean and consistent internal representation, and writing it into a SQLite database. The pipeline is designed to be simple, reliable, and easy to extend.

### Key Features
- **Modular design**: Clear separation between fetching, transforming, and storing data  
- **Idempotent operations**: Safe to run multiple times without creating duplicates  
- **Error handling**: Retry logic for network requests and structured logging  
- **Configuration**: Environment-based settings via \`.env\`  
- **Self-contained**: Runs locally with no external infrastructure required  

---

## Architecture

```text
prima-bi-metadata/
├── src/prima_bi_metadata/
│   ├── config.py           # Configuration management
│   ├── main.py             # ETL orchestration
│   ├── transform.py        # Data normalization
│   └── storage.py          # Database operations
├── data/
│   └── sample_data.json    # Sample input data
├── tests/
│   ├── test_check_db.py    # Database upsert tests
│   └── test_show_schema.py # Schema validation tests
├── metadata.db             # SQLite database
└── pyproject.toml          # Dependencies and project config
```


### Components
- **Configuration**: Managed through environment variables  
- **Logging**: Structured logging with timestamps and levels  
- **Database**: SQLite with SQLAlchemy for upsert operations  
- **Dependencies**: Managed via Poetry  

---

## Data Flow

### 1. Fetch
\`main.py\` retrieves metadata from a single JSON file served over HTTP.

- Uses \`requests\` with retry logic  
- Logs request attempts and outcomes  
- Supports local development via \`python -m http.server\`  

### 2. Transform
\`transform.py\` normalizes raw JSON into a pandas DataFrame:

- Maps fields to the internal schema  
- Handles missing values  
- Converts timestamps to consistent formats  

### 3. Upsert
\`storage.py\` writes transformed data into SQLite:

- Uses SQLAlchemy  
- Performs idempotent upserts via \`ON CONFLICT\`  
- Converts NaN → None  
- Ensures transactional integrity  

---

## Database Schema
```text
sql
CREATE TABLE metadata (
    asset_id VARCHAR NOT NULL PRIMARY KEY,
    asset_name VARCHAR,
    owner VARCHAR,
    last_updated DATETIME,
    last_viewed DATETIME,
    views_last_30d INTEGER,
    last_refresh DATETIME,
    refresh_status VARCHAR,
    status VARCHAR,
    last_synced_at DATETIME
);
```

---

## What Has Been Implemented

### Complete ETL Pipeline
- Fetch from a single JSON file served via HTTP
- Data transformation and normalization
- Idempotent database storage

### Core Modules
- config.py: Environment-based configuration
- main.py: Pipeline orchestration and logging
- transform.py: Data normalization
- storage.py: Database upsert logic

### Quality Assurance
- Test suite for schema and upsert behavior
- Sample data for development
- Structured logging

### Infrastructure
- SQLite database
- Poetry dependency management
- .env configuration support

---

## Running the Pipeline

### Setup
Install dependencies with Poetry:

bash
poetry install


### Configuration
Create a .env file in the project root:

```text
BI_API_BASE=http://localhost:8000/data/sample_data.json
DB_URL=sqlite:///metadata.db
LOG_LEVEL=INFO
```

### Serve the sample data
Start a local HTTP server in the project root:

bash
python -m http.server 8000


### Run the pipeline

bash
poetry run python -m prima_bi_metadata.main

### To inspect the database without installing SQLite, run:

bash
poetry run python scripts/show_db.py

---

## Monitoring & Alerting

A BI metadata pipeline benefits from proactive monitoring to ensure dashboards remain fresh, reliable, and actively used. Based on the available fields, recommended alerts include:

- Stale dashboards: Trigger when last_viewed or last_updated exceeds a threshold (e.g., 30–60 days)
- Failed refreshes: Alert when refresh_status indicates failure or when last_refresh is older than expected
- Usage anomalies: Detect sudden drops in views_last_30d
- Data freshness issues: Monitor last_synced_at to ensure the pipeline runs regularly
- Owner notifications: Notify dashboard owners when their assets become stale or fail to refresh

---

## Design Decisions

### Why SQLite?
SQLite is lightweight, file-based, and requires no external infrastructure. It supports primary keys and ON CONFLICT upserts, enabling clean idempotent writes. This makes the project easy to run locally and ideal for evaluation.

### Why a modular ETL?
Separating fetch, transform, and storage logic improves maintainability and testability. Each component can evolve independently.

### Why pandas for transformation?
Pandas simplifies type coercion, timestamp normalization, and handling missing values.

---

## Data Governance

A robust BI environment requires consistent definitions, ownership, and lineage. This pipeline supports governance by centralizing metadata.

Key practices include:
- Semantic layer consistency
- Version control for metrics
- Ownership clarity
- Lineage tracking
- Deprecation workflows
- Quality checks

---

## Dashboard Design Thinking

A monitoring dashboard built on this metadata should include:

- Active dashboards over time
- Stale dashboards
- Failed refreshes
- Usage distribution
- Owner activity
- Refresh latency
- Status breakdown
- Top dashboards by views