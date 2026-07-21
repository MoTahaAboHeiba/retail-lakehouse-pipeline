import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(os.getenv("POSTGRES_CONNECTION_STRING"))
cur = conn.cursor()

# PK column for each table (for ON CONFLICT DO NOTHING)
table_pk = {
    "raw.customers": "customer_id",
    "raw.stores": "store_id",
    "raw.products": "product_id",
    "raw.employees": "employee_id",
    "raw.orders": "order_id",
    "raw.order_items": "order_item_id",
}

csv_files = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}

data_dir = os.path.join(os.path.dirname(__file__), "dataset", "data")

for csv_file, table_name in csv_files.items():
    csv_path = os.path.join(data_dir, csv_file)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    pk = table_pk[table_name]
    staging_table = f"_staging_{csv_file.replace('.csv', '')}"

    print(f"Processing {csv_file} into {table_name} (incremental)...")

    # Create temporary staging table with same structure (no PK constraint to avoid COPY conflicts)
    cur.execute(f"""
        CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS)
    """)

    # Load CSV into staging table
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        cur.copy_expert(f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)

    # Get column names from the target table (COPY does not populate cur.description)
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = '{table_name.replace("raw.", "")}'
        ORDER BY ordinal_position
    """)
    columns = [row[0] for row in cur.fetchall()]
    col_names = ", ".join(columns)

    # Insert only new records (skip existing ones by PK)
    cur.execute(f"""
        INSERT INTO {table_name} ({col_names})
        SELECT {col_names}
        FROM {staging_table}
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} t WHERE t.{pk} = {staging_table}.{pk}
        )
    """)

    inserted = cur.rowcount
    conn.commit()

    # Drop staging table
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

    print(f"  Inserted {inserted} new row(s) into {table_name}")

cur.close()
conn.close()
print("All CSV files loaded incrementally.")
