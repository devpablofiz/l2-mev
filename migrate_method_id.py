import os
import psycopg2
from dotenv import load_dotenv
import logging
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "l2_mev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def migrate():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        logger.info("Starting migration...")

        # 1. Add method_id column if it doesn't exist
        logger.info("Adding method_id column...")
        cur.execute("""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='transactions' AND column_name='method_id') THEN
                    ALTER TABLE transactions ADD COLUMN method_id TEXT;
                END IF;
            END $$;
        """)

        # 2. Populate method_id for existing rows
        logger.info("Populating method_id column for existing rows...")
        cur.execute("""
            UPDATE transactions 
            SET method_id = SUBSTRING(input_data, 1, 10)
            WHERE method_id IS NULL AND input_data IS NOT NULL;
        """)
        logger.info(f"Updated {cur.rowcount} rows.")

        # 3. Create index
        logger.info("Creating index on method_id...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_method_id ON transactions (method_id);")

        # 4. Drop old substring index if it exists
        logger.info("Dropping old substring index if exists...")
        cur.execute("DROP INDEX IF EXISTS idx_transactions_method_id_old;") # In case it was named differently or we want to cleanup

        logger.info("Migration completed successfully.")
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate()
