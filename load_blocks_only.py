import os
import sys
import logging
import psycopg2
import time
from psycopg2 import extras
from web3 import Web3
from web3.datastructures import AttributeDict
from eth_utils import to_hex
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
RPC_URL = os.getenv("RPC_URL")
ALCHEMY_RPC_URL = os.getenv("ALCHEMY_RPC_URL")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "l2_mev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
RPC_DELAY = float(os.getenv("RPC_DELAY", "0.1"))

# Alchemy CU Constants (if applicable, though we only use getBlockByNumber)
ALCHEMY_CU_LIMIT = int(os.getenv("ALCHEMY_CU_LIMIT", "500"))
CU_COST_BLOCK = int(os.getenv("CU_COST_BLOCK", "16"))

class CURateLimiter:
    def __init__(self, cu_limit):
        self.cu_limit = cu_limit
        self.last_reset = time.time()
        self.used_cu = 0

    def wait_for_cu(self, cu_needed):
        """Wait if the requested CU would exceed the per-second limit."""
        current_time = time.time()
        elapsed = current_time - self.last_reset
        
        # Reset limit every second
        if elapsed >= 1.0:
            self.used_cu = 0
            self.last_reset = current_time
        
        if self.used_cu + cu_needed > self.cu_limit:
            wait_time = 1.0 - elapsed
            if wait_time > 0:
                logger.debug(f"CU limit reached, waiting {wait_time:.2f}s...")
                time.sleep(wait_time)
            self.used_cu = 0
            self.last_reset = time.time()
        
        self.used_cu += cu_needed

# Initialize the global limiter
alchemy_limiter = CURateLimiter(ALCHEMY_CU_LIMIT)

def retry_rpc_call(func, *args, max_retries=5, initial_delay=1, cu_cost=0, **kwargs):
    """Retry an RPC call with exponential backoff on rate limit errors and CU management."""
    for i in range(max_retries):
        try:
            # Check the limiter if we have a CU cost
            if cu_cost > 0:
                alchemy_limiter.wait_for_cu(cu_cost)
            
            return func(*args, **kwargs)
        except Exception as e:
            error_str = str(e)
            if "limit reached" in error_str or "-32007" in error_str or "429" in error_str:
                delay = initial_delay * (2 ** i)
                logger.warning(f"Rate limit hit, retrying in {delay}s... (Attempt {i+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise e
    return func(*args, **kwargs)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        sys.exit(1)

def fetch_and_store_block_only(w3, conn, block_number):
    try:
        # Get block with full transactions - eth_getBlockByNumber costs 16 CUs
        block = retry_rpc_call(w3.eth.get_block, block_number, full_transactions=True, cu_cost=CU_COST_BLOCK)
        time.sleep(RPC_DELAY)
        timestamp = datetime.fromtimestamp(block['timestamp'])
        
        with conn.cursor() as cur:
            # Insert block
            cur.execute(
                "INSERT INTO blocks (block_number, block_hash, timestamp) VALUES (%s, %s, %s) ON CONFLICT (block_number) DO NOTHING",
                (block['number'], block['hash'].hex(), timestamp)
            )
            
            # Prepare transactions for batch insert
            tx_data = []
            
            for tx in block['transactions']:
                tx_hash_hex = tx['hash'].hex()
                
                tx_data.append((
                    tx_hash_hex,
                    block['number'],
                    tx['from'],
                    tx['to'],
                    float(w3.from_wei(tx['value'], 'ether')),
                    tx['gas'],
                    tx.get('gasPrice'),
                    tx.get('maxFeePerGas'),
                    tx.get('maxPriorityFeePerGas'),
                    tx['input'].hex() if isinstance(tx['input'], bytes) else tx['input'],
                    tx['nonce'],
                    tx['transactionIndex'],
                    None # status is not available without receipt
                ))
            
            if tx_data:
                extras.execute_values(
                    cur,
                    "INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas, input_data, nonce, transaction_index, status) VALUES %s ON CONFLICT (tx_hash) DO NOTHING",
                    tx_data
                )
        
        conn.commit()
        logger.info(f"Successfully loaded block {block_number} with {len(block['transactions'])} transactions (no receipts).")
        return True
    except Exception as e:
        logger.error(f"Error loading block {block_number}: {e}")
        conn.rollback()
        return False

def main(start_block, end_block):
    if not RPC_URL:
        logger.error("RPC_URL not found in environment variables.")
        return

    w3 = Web3(Web3.HTTPProvider(ALCHEMY_RPC_URL))
    if not w3.is_connected():
        logger.error("Failed to connect to RPC.")
        return
    
    current_block = retry_rpc_call(lambda: w3.eth.block_number)
    time.sleep(RPC_DELAY)
    logger.info(f"Connected to RPC. Current block: {current_block}")
    
    conn = get_db_connection()
    
    for block_num in range(start_block, end_block + 1):
        fetch_and_store_block_only(w3, conn, block_num)
    
    conn.close()
    logger.info("Finished loading blocks.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python load_blocks_only.py <start_block> <end_block>")
        sys.exit(1)
        
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    main(start, end)
