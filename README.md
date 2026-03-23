## PRIMA‑BI‑METADATA

PRIMA‑BI‑METADATA is a lightweight ETL pipeline that ingests BI metadata, normalizes it into a structured internal model, and persists it into a local SQLite database using an idempotent upsert strategy. The project follows a modular architecture and supports incremental, repeatable data refreshes.

### Project Overview

The goal of this project is to maintain an up‑to‑date local metadata store by fetching BI metadata from an external source (API or JSON file), transforming it into a clean and consistent internal representation, and writing it into a SQLite database. The pipeline is designed to be simple, reliable, and easy to extend.

Key features:
- **Modular design**: Separate concerns for fetching, transforming, and storing data
- **Idempotent operations**: Safe to run multiple times without duplicates
- **Error handling**: Retry logic for network requests, comprehensive logging
- **Configuration**: Environment-based settings for API endpoints and credentials

---

### Architecture

The repository is organized into clear, single‑responsibility modules:

```
prima-bi-metadata/
├── src/prima_bi_metadata/
│   ├── config.py          # Configuration management
│   ├── main.py            # ETL orchestration
│   ├── transform.py       # Data normalization
│   └── storage.py         # Database operations
├── data/
│   └── sample_data.json   # Sample input data
├── tests/
│   ├── test_check_db.py   # Database upsert tests
│   └── test_show_schema.py # Schema validation tests
├── metadata.db            # SQLite database
└── pyproject.toml         # Dependencies and project config
```

- **Configuration**: Managed through environment variables in `.env` file
- **Logging**: Structured logging with configurable levels
- **Database**: SQLite with SQLAlchemy for ORM operations
- **Dependencies**: Managed via Poetry with `pyproject.toml`

---

### Data Flow

#### 1. Fetch
`main.py` retrieves metadata from a configured endpoint:
- Supports both paginated APIs and single JSON files
- Handles authentication via Bearer tokens
- Implements retry logic with exponential backoff
- Logs request details and record counts

#### 2. Transform
`transform.py` normalizes raw JSON into a clean DataFrame:
- Maps API fields to internal schema
- Handles missing/null values
- Parses and normalizes timestamps
- Derives status fields (active/stale/failed_refresh)
- Validates data types and formats

#### 3. Upsert
`storage.py` writes transformed data into SQLite:
- Bulk insert/update operations
- `ON CONFLICT` clause ensures idempotency
- Safe type conversion (NaN→None, timestamps→ISO strings)
- Transaction handling for data integrity
- Returns count of processed records

---

### Database Schema

The `metadata` table stores BI asset information:

```sql
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

**Field descriptions:**
- `asset_id`: Unique identifier for the BI asset
- `asset_name`: Human-readable name
- `owner`: Email or identifier of the asset owner
- `last_updated`: When the asset was last modified
- `last_viewed`: When the asset was last accessed
- `views_last_30d`: Number of views in the last 30 days
- `last_refresh`: Timestamp of last data refresh
- `refresh_status`: Status of the refresh operation
- `status`: Derived status (active/stale/failed_refresh)
- `last_synced_at`: When this record was last synced

---

### What Has Been Implemented

**Complete ETL Pipeline**
- Fetch from API/JSON with pagination support
- Data transformation and validation
- Idempotent database storage

**Core Modules**
- `config.py`: Environment-based configuration
- `main.py`: Pipeline orchestration with error handling
- `transform.py`: Data normalization and mapping
- `storage.py`: Database operations with upsert logic

**Quality Assurance**
- Comprehensive test suite for database operations
- Sample data for testing and development
- Logging and error handling throughout

**Infrastructure**
- SQLite database with proper schema
- Dependency management via `pyproject.toml`
- Environment configuration support

---

### Running the Pipeline

#### Prerequisites

View pyproject.toml

#### Setup
Install dependencies with Poetry:
```bash
poetry install
```

#### Configuration
Create a `.env` file in the project root:
```
BI_API_BASE=http://localhost:8000/data/sample_data.json
BI_API_KEY=
DB_URL=sqlite:///metadata.db
LOG_LEVEL=INFO
```

#### Execution

For development with sample data, you need to:

1. Start a local HTTP server in the project root:
```bash
python -m http.server 8000
```

2. Set `BI_API_BASE` in your `.env` file to:
```
BI_API_BASE=http://localhost:8000/data/sample_data.json
```

This serves the `sample_data.json` file via HTTP, which the pipeline will fetch and process.

Run the full pipeline:
```bash
poetry run python -m prima_bi_metadata.main


### Database Schema

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
