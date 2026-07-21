# Walmart Retail Database

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![PostgreSQL 16+](https://img.shields.io/badge/postgresql-16%2B-336791.svg)](https://www.postgresql.org/)
[![Ghost](https://img.shields.io/badge/ghost-TimescaleDB-1E90FF.svg)](https://ghost.build)
[![UV](https://img.shields.io/badge/package_manager-uv-4B8BBE.svg)](https://docs.astral.sh/uv/)

**walmart_db** is a lightweight, idempotent data ingestion engine that bulk-loads Walmart retail CSV datasets into PostgreSQL. Designed from a **Data Engineer's mindset** — incremental loads, schema-as-code, `COPY`-based ingestion, and zero side effects on re-runs.

> **Part of a larger ecosystem.** This database is the **source layer** in a broader analytics pipeline. The downstream consumer is **Databricks via [LakeFlow Connect](https://www.databricks.com/product/lakeflow)** — raw data flows from PostgreSQL to Databricks for transformation, enrichment, and serving to dashboards and ML models. This repo focuses only on the ingestion side so you can run it standalone.

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

## Architecture

```
Agentic DB/
├── .env.example                  # Template — copy to .env and fill credentials
├── setup_db.py                   # — DDL executor (one-time schema setup)
├── load_data.py                  # — Incremental loader (idempotent)
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

**Key design decisions:**
- **Schema-as-code** — DDL lives in version control alongside the data pipeline.
- **Idempotent loads** — `INSERT ... WHERE NOT EXISTS` ensures duplicate-safe ingestion.
- **Bulk `COPY`** — CSV data is loaded via PostgreSQL's native `COPY FROM STDIN` into temp staging tables, then merged.
- **Zero config** — single `.env` variable drives the entire pipeline.

---

## What is Ghost.build?

[Ghost](https://ghost.build) is a PostgreSQL platform built for AI-native workflows. Think of it as **GitHub + Docker + PostgreSQL** — every user gets an isolated, disposable database instance instead of sharing a fragile staging environment.

**The problem it solves:**

```
  Traditional:                  Ghost:
  Production DB               Production DB
       |                           |
       v                           v
   Staging DB             Fork — Isolated DB per user
       |                           |
       |                     ┌─────┴────────┐
       |                     │              │
  Shared, risky             Agent A      Agent B
                            Own DB       Own DB
```

**Key capabilities relevant to this project:**

| Feature | What it does |
|---------|--------------|
| `ghost create` | Spin up a PostgreSQL database in seconds — no VM provisioning |
| `ghost fork <db>` | Clone any database into an independent copy (like `git branch` for data) |
| `ghost delete` | Destroy a database instantly — zero cleanup overhead |
| `ghost schema` | AI-friendly schema introspection (reduces hallucinations) |
| MCP integration | AI agents (Claude, Cursor, Codex) can manage databases autonomously |

---

## Ghost CLI Commands

```bash
# One-time setup — configure PATH, login, MCP, and shell completions
ghost init

# Create a new TimescaleDB-backed PostgreSQL database
ghost create

# Fork an existing database — identical data + schema, independent changes
ghost fork walmart_db

# List all databases in your Ghost account
ghost list

# Open a local web UI for ad-hoc SQL queries
ghost serve
```

> **Tip:** Run `ghost init` once. Use `ghost create` to provision, then grab the connection string for your `.env`.

---

## Database Forking and Read-Only Access

A **read-only fork** (`walmart_db_fork`) has been created from the primary `walmart_db` so users can explore the dataset safely without risking changes to the source.

```bash
ghost list
```

```
ID          NAME               STATUS   STORAGE  COMPUTE
DB1_ID  walmart_db         running  655MiB   60.25h
BD2_ID  walmart_db_fork    running  670MiB   0.25h    <- read-only fork
```

To create your own fork from the original database:

```bash
ghost fork walmart_db --name my_experiment
```

This gives you an independent copy with:
- **Identical data** — all 42,796 records
- **Identical schema** — same tables, indexes, constraints
- **Isolated writes** — your changes never affect the original

When you are done, destroy it without leaving a trace:

```bash
ghost delete my_experiment
```

> This follows the **Git-branching-for-databases** pattern: fork, experiment, validate, delete. No shared state, no risk.

---

## Run on Your Device

### Prerequisites

- **Python 3.12+**
- **UV** — [Install UV](https://docs.astral.sh/uv/#installation)
- **Ghost CLI** — [Install Ghost](https://ghost.build/docs/#installation)
- A **PostgreSQL-compatible** database (Ghost, TimescaleDB, Supabase, or local Postgres)

### 1. Clone and enter project

```bash
cd 'your project folder'
```

### 2. Create environment and install dependencies

```bash
uv venv
source .venv/bin/activate      # Linux / macOS
.venv\Scripts\activate          # Windows

uv sync
```

### 3. Provision a database (Ghost)

```bash
ghost init          # One-time setup: login and configure
ghost create        # Create a new TimescaleDB database
ghost list          # Copy your connection string
```

### 4. Configure connection

Copy the template and fill in your credentials:

```bash
cp .env.example .env      # Linux / macOS
copy .env.example .env     # Windows
```

```env
POSTGRES_CONNECTION_STRING = postgresql://user:password@host:5432/tsdb?sslmode=require
POSTGRES_READONLY_CONNECTION_STRING = postgresql://readonly_user:password@host:5432/tsdb?sslmode=require
```

> **Security:** `.env` is gitignored. Never commit credentials.

### 5. Execute DDL (one-time)

```bash
python setup_db.py
```

This creates the `raw` schema and all 6 tables. On success, you will see:

```
Tables created in `raw` schema: customers, employees, order_items, orders, products, stores
```

### 6. Load data

```bash
python load_data.py
```

**First run output:**

```
Processing customers.csv into raw.customers (incremental)...
  Inserted 2000 new row(s) into raw.customers
Processing order_items.csv into raw.order_items (incremental)...
  Inserted 30021 new row(s) into raw.order_items
...
All CSV files loaded incrementally.
```

**Re-run output** (idempotent — zero new rows):

```
Processing customers.csv into raw.customers (incremental)...
  Inserted 0 new row(s) into raw.customers
...
All CSV files loaded incrementally.
```

### 7. Explore the data

Use `ghost serve` to open a web-based SQL editor and query the loaded data directly:

```bash
ghost serve
```

---

## How It Works

`load_data.py` follows an **ELT-style incremental load pattern**:

1. **Read** the connection string from `.env`
2. **Create** a temp staging table mirroring the target table's structure (no PK constraints — avoids `COPY` conflicts)
3. **Bulk-load** the CSV into staging via `COPY FROM STDIN`
4. **Merge** — `INSERT ... SELECT ... WHERE NOT EXISTS` inserts only new records based on the primary key
5. **Clean up** — staging tables are dropped automatically

> **Immutable by default:** Existing rows are never modified. Only new primary keys trigger inserts. Rerun the script 100 times — same result as the first run.

---

## Pipeline Extensibility

| Goal | How |
|------|-----|
| **Add a new table** | Add DDL to `dataset/ddl/walmart_schema.sql`, place CSV in `dataset/data/`, register it in `load_data.py`'s `csv_files` and `table_pk` dicts |
| **Change target schema** | Update `table_schema = 'raw'` in the `information_schema.columns` query inside `load_data.py` |
| **Point to a different DB** | Swap the `POSTGRES_CONNECTION_STRING` in `.env` — no code changes needed |

---

## Dependencies

| Package | Role |
|---------|------|
| `psycopg2-binary` | PostgreSQL adapter (raw SQL execution, COPY protocol) |
| `python-dotenv` | Load environment variables from `.env` |

---
