# PostgreSQL Database 

## Overview
This project delivers a repeatable, data-centric pipeline for ingesting Walmart dataset CSV files into a structured Python-based workflow. It includes a data loading utility, a schema definition, and a simple data import script that brings CSV source data into a usable environment.

## What We Built
- `import_csv.py`: A Python script that processes and imports CSV files into a target destination.
- `dataset/load_data.py`: Dataset loading logic for the Walmart CSV files.
- `dataset/ddl/walmart_schema.sql`: A schema definition that models Walmart business entities such as customers, employees, orders, products, stores, and order items.
- `dataset/data/*.csv`: Source data files for the Walmart dataset.

## Business Value
Using a structured approach, this project transforms raw CSV files into a reliable, documented data pipeline. That enables business teams to:
- Reduce manual data preparation effort.
- Improve data consistency and quality through a defined schema.
- Accelerate analysis and reporting by making Walmart dataset assets easier to ingest and reuse.

## Why This Matters
From a data engineering perspective, this work establishes the foundation for analytics and operational reporting. It allows the business to move from ad hoc CSV handling toward a repeatable, maintainable ingestion process, which is critical for scaling data initiatives and supporting future automation.

## How to Use
1. Review the schema in `dataset/ddl/walmart_schema.sql` to understand the data model.
2. Place the source CSV files in `dataset/data/`.
3. Run `python load_data.py` to load the data using the project pipeline.

## Summary
This project is a practical, business-ready data ingestion solution that turns raw Walmart CSV data into a structured pipeline. It is designed to support downstream analytics, reporting, and further data engineering work by creating a stable entry point for the dataset.
