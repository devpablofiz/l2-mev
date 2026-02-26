import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# DB Connection Details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "l2_mev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_sqlalchemy_engine():
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def ensure_method_frequencies_view():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_matviews WHERE matviewname = 'method_frequencies'")
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute("""
        CREATE MATERIALIZED VIEW method_frequencies AS
        SELECT 
            method_id,
            COUNT(*) AS usage_count,
            MIN(tx_hash) AS sample_tx_hash,
            MAX(block_number) AS last_seen_block
        FROM transactions
        WHERE method_id IS NOT NULL AND method_id <> '0x'
        GROUP BY method_id
        """)
        conn.commit()
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS method_frequencies_method_id_idx ON method_frequencies (method_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS method_frequencies_usage_count_idx ON method_frequencies (usage_count DESC)")
    conn.commit()
    cur.close()
    conn.close()

def refresh_method_frequencies(concurrently: bool = True):
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    if concurrently:
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY method_frequencies")
    else:
        cur.execute("REFRESH MATERIALIZED VIEW method_frequencies")
    cur.close()
    conn.close()
