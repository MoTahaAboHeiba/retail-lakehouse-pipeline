# PostgreSQL source layer

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![PostgreSQL 16+](https://img.shields.io/badge/postgresql-16%2B-336791.svg)](https://www.postgresql.org/)
[![Ghost](https://img.shields.io/badge/ghost-TimescaleDB-1E90FF.svg)](https://ghost.build)
[![UV](https://img.shields.io/badge/package_manager-uv-4B8BBE.svg)](https://docs.astral.sh/uv/)
[![Databricks](https://img.shields.io/badge/databricks-downstream-FF3621.svg)](https://www.databricks.com)

Idempotent data ingestion engine that bulk-loads six retail CSV datasets into PostgreSQL. Schema-as-code, `COPY`-based ingestion, zero side effects on re-runs. This is the source layer in the pipeline. Downstream consumer is Databricks via Lakeflow Connect.

---

## Dataset

| File | Table |
|------|-------|
| `customers.csv` | `raw.customers` |
| `stores.csv` | `raw.stores` |
| `products.csv` | `raw.products` |
| `employees.csv` | `raw.employees` |
| `orders.csv` | `raw.orders` |
| `order_items.csv` | `raw.order_items` |

---

## Ingestion flow

```mermaid
flowchart LR
    csv["CSV files\n6 source tables"]
    stg["Temp staging table\nno PK constraints"]
    raw["raw schema\nPostgreSQL on Ghost.build"]
    db["Databricks Bronze\nLakeflow Connect"]

    csv -->|"COPY FROM STDIN"| stg
    stg -->|"INSERT WHERE NOT EXISTS"| raw
    raw -->|"cursor column + PK upsert\nscheduled"| db
```

---

## Architecture

```text
postgres/
├── .env.example                  # Template — copy to .env and fill credentials
├── setup_db.py                   # DDL executor (one-time schema setup)
├── load_data.py                  # Incremental loader (idempotent)
└── dataset/
    ├── ddl/
    │   └── walmart_schema.sql    # Schema-as-code: raw.customers, raw.stores, ...
    └── data/
        ├── customers.csv
        ├── employees.csv
        ├── order_items.csv
        ├── orders.csv
        ├── products.csv
        └── stores.csv
```

Key design decisions:

- **Schema-as-code:** DDL lives in version control alongside the data pipeline.
- **Idempotent loads:** `INSERT ... WHERE NOT EXISTS` ensures duplicate-safe ingestion.
- **Bulk `COPY`:** CSV data loads via PostgreSQL's native `COPY FROM STDIN` into temp staging tables, then merges into the target.
- **Zero config:** a single `.env` variable drives the entire pipeline.

---

## What is Ghost.build?

[Ghost](https://ghost.build) is a PostgreSQL platform that gives every user an isolated, disposable database instance instead of a shared staging environment.

```text
Traditional:                  Ghost:
Production DB               Production DB
     |                           |
     v                           v
 Staging DB             Fork — isolated DB per user
     |                           |
     |                     ┌─────┴────────┐
     |                     │              │
Shared, risky          Instance A     Instance B
                           Own DB       Own DB
```

| Feature | What it does |
|---------|--------------|
| `ghost create` | Provisions a PostgreSQL database in seconds |
| `ghost fork <db>` | Clones any database into an independent copy |
| `ghost delete` | Destroys a database instantly, zero cleanup overhead |
| `ghost schema` | Schema introspection for AI-friendly tooling |
| MCP integration | AI agents can manage databases autonomously |

---

## Ghost CLI commands

```bash
# One-time setup: configure PATH, login, MCP, and shell completions
ghost init

# Create a new TimescaleDB-backed PostgreSQL database
ghost create

# Fork an existing database: identical data and schema, independent changes
ghost fork retail_db

# List all databases in your Ghost account
ghost list

# Open a local web UI for ad-hoc SQL queries
ghost serve
```

---

## Database forking and read-only access

A read-only fork (`retail_db_fork`) has been created from the primary `retail_db` so you can explore the dataset without affecting the source.

```bash
ghost list
```

```
ID          NAME               STATUS   STORAGE  COMPUTE
DB1_ID  retail_db          running  655MiB   60.25h
DB2_ID  retail_db_fork     running  670MiB   0.25h    <- read-only fork
```

To create your own fork:

```bash
ghost fork retail_db --name my_experiment
```

This gives you an independent copy with:

- Identical data: all 42,796 records
- Identical schema: same tables, indexes, constraints
- Isolated writes: your changes never affect the original

When you are done:

```bash
ghost delete my_experiment
```

Fork, experiment, validate, delete. No shared state, no risk.

---

## Run on your device

### Prerequisites

- Python 3.12+
- [UV](https://docs.astral.sh/uv/#installation)
- [Ghost CLI](https://ghost.build/docs/#installation)
- A PostgreSQL-compatible database (Ghost, Supabase, or local Postgres)

### 1. Clone and enter project

```bash
cd postgres
```

### 2. Create environment and install dependencies

```bash
uv venv
source .venv/bin/activate      # Linux / macOS
.venv\Scripts\activate          # Windows

uv sync
```

### 3. Provision a database

```bash
ghost init          # One-time: login and configure
ghost create        # Provision a new TimescaleDB database
ghost list          # Copy the connection string
```

### 4. Configure connection

```bash
cp .env.example .env
```

```env
POSTGRES_CONNECTION_STRING = postgresql://user:password@host:5432/tsdb?sslmode=require
POSTGRES_READONLY_CONNECTION_STRING = postgresql://readonly_user:password@host:5432/tsdb?sslmode=require
```

`.env` is gitignored. Never commit credentials.

### 5. Execute DDL (one-time)

```bash
python setup_db.py
```

Creates the `raw` schema and all 6 tables:

```
Tables created in `raw` schema: customers, employees, order_items, orders, products, stores
```

### 6. Load data

```bash
python load_data.py
```

First run:

```
Processing customers.csv into raw.customers (incremental)...
  Inserted 2000 new row(s) into raw.customers
Processing order_items.csv into raw.order_items (incremental)...
  Inserted 30021 new row(s) into raw.order_items
...
All CSV files loaded incrementally.
```

Re-run (idempotent, zero new rows):

```
Processing customers.csv into raw.customers (incremental)...
  Inserted 0 new row(s) into raw.customers
...
All CSV files loaded incrementally.
```

### 7. Explore the data

```bash
ghost serve
```

Opens a web-based SQL editor pointed at your database.

---

## How it works

`load_data.py` runs a five-step incremental load per table:

1. Read the connection string from `.env`
2. Create a temp staging table mirroring the target structure with no PK constraints, avoiding `COPY` conflicts
3. Bulk-load the CSV into staging via `COPY FROM STDIN`
4. Merge: `INSERT ... SELECT ... WHERE NOT EXISTS` inserts only records whose primary key is absent from the target
5. Drop the staging table

Existing rows are never modified. Only new primary keys trigger inserts. The script is safe to re-run any number of times.

---

## Pipeline extensibility

| Goal | How |
|------|-----|
| Add a new table | Add DDL to `dataset/ddl/walmart_schema.sql`, place the CSV in `dataset/data/`, register it in `load_data.py`'s `csv_files` and `table_pk` dicts |
| Change target schema | Update `table_schema = 'raw'` in the `information_schema.columns` query inside `load_data.py` |
| Point to a different database | Swap `POSTGRES_CONNECTION_STRING` in `.env`, no code changes needed |

---

## Dependencies

| Package | Role |
|---------|------|
| `psycopg2-binary` | PostgreSQL adapter: raw SQL execution and COPY protocol |
| `python-dotenv` | Loads environment variables from `.env`
