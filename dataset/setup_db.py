"""
Run this script to:
1. Connect to the Ghost.build PostgreSQL database
2. Execute the DDL (walmart_schema.sql) to create schema & tables
3. Verify tables were created
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONN_STRING = os.getenv("POSTGRES_CONNECTION_STRING")
if not CONN_STRING:
    print("ERROR: POSTGRES_CONNECTION_STRING not found in .env")
    print("Please add it to Agentic DB/.env")
    exit(1)

print(f"Connecting to: {CONN_STRING[:40]}...")

try:
    conn = psycopg2.connect(CONN_STRING)
    conn.autocommit = True
    cur = conn.cursor()
    print("Connected to Ghost.build PostgreSQL successfully!")
    
    # Read and execute DDL
    ddl_path = os.path.join(os.path.dirname(__file__), "dataset", "ddl", "walmart_schema.sql")
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()
    
    print("\n Executing DDL to create schema and tables...")
    cur.execute(ddl_sql)
    print("DDL executed successfully! Schema `raw` and all 6 tables created.")
    
    # Verify tables
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'raw' 
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    print(f"\n Tables created in `raw` schema: {', '.join(tables)}")
    
    # Row counts
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM raw.{table}")
        count = cur.fetchone()[0]
        print(f"   raw.{table}: {count} rows")
    
    cur.close()
    conn.close()
    print("\n Database setup complete! Now run: python load_data.py")
    
except Exception as e:
    print(f" Error: {e}")
    exit(1)

