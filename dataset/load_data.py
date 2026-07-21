import os
import psycopg2

conn = psycopg2.connect("PUT YOUR POSTGRES CONNECTION STRING HERE")
cur = conn.cursor()

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

    print(f"Loading {csv_file} into {table_name}")
    
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
    conn.commit()
    print(f"Loaded {csv_file}")

cur.close()
conn.close()
print("All CSV files loaded successfully.")
