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
ALCHEMY_RPC_URL = os.getenv("ALCHEMY_RPC_URL") # Secondary RPC for receipts
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "l2_mev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
RPC_DELAY = float(os.getenv("RPC_DELAY"))  # Delay in seconds after each RPC call
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))    # Number of receipts to fetch in one batch

# Alchemy CU Constants (moved to env)
ALCHEMY_CU_LIMIT = int(os.getenv("ALCHEMY_CU_LIMIT"))  # CU per second limit
CU_COST_RECEIPT = int(os.getenv("CU_COST_RECEIPT"))    # eth_getTransactionReceipt cost
CU_COST_BLOCK = int(os.getenv("CU_COST_BLOCK"))      # eth_getBlockByNumber cost

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
            # If using Alchemy and we have a CU cost, check the limiter
            if ALCHEMY_RPC_URL and cu_cost > 0:
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

def web3_to_json(obj):
    """Convert Web3 objects to JSON serializable format."""
    if isinstance(obj, AttributeDict):
        return {k: web3_to_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [web3_to_json(x) for x in obj]
    elif isinstance(obj, bytes):
        return to_hex(obj)
    return obj

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

def init_db(conn):
    try:
        conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        conn.rollback()
        sys.exit(1)

def fetch_and_store_block(w3, w3_receipts, conn, block_number):
    try:
        # Get block (Primary RPC) - eth_getBlockByNumber costs 16 CUs
        block = retry_rpc_call(w3.eth.get_block, block_number, full_transactions=True, cu_cost=CU_COST_BLOCK)
        time.sleep(RPC_DELAY)
        timestamp = datetime.fromtimestamp(block['timestamp'])
        
        with conn.cursor() as cur:
            # Insert block
            cur.execute(
                "INSERT INTO blocks (block_number, block_hash, timestamp) VALUES (%s, %s, %s) ON CONFLICT (block_number) DO NOTHING",
                (block['number'], block['hash'].hex(), timestamp)
            )
            
            # Prepare transactions and logs for batch insert
            tx_data = []
            log_data = []
            
            # Fetch receipts in smaller chunks to respect rate limits
            receipts = []
            if block['transactions']:
                all_txs = list(block['transactions'])
                logger.info(f"Fetching receipts for {len(all_txs)} transactions in chunks of {BATCH_SIZE} using {'Alchemy' if ALCHEMY_RPC_URL else 'Primary'} RPC...")
                
                for i in range(0, len(all_txs), BATCH_SIZE):
                    chunk = all_txs[i:i + BATCH_SIZE]
                    cu_cost = len(chunk) * CU_COST_RECEIPT  # Calculate total CU cost for this batch
                    
                    def execute_batch_chunk(chunk_to_fetch):
                        with w3_receipts.batch_requests() as batch:
                            for tx in chunk_to_fetch:
                                batch.add(w3_receipts.eth.get_transaction_receipt(tx['hash']))
                            return batch.execute()
                    
                    # Pass the calculated CU cost to the retry wrapper
                    chunk_receipts = retry_rpc_call(execute_batch_chunk, chunk, cu_cost=cu_cost)
                    receipts.extend(chunk_receipts)
                    time.sleep(RPC_DELAY)
            
            for tx, receipt in zip(block['transactions'], receipts):
                if receipt is None:
                    logger.warning(f"Receipt not found for transaction {tx['hash'].hex()}")
                    continue
                    
                tx_hash_hex = tx['hash'].hex()
                
                # Process logs
                for log in receipt.get('logs', []):
                    topics = log.get('topics', [])
                    log_data.append((
                        tx_hash_hex,
                        log['address'],
                        to_hex(topics[0]) if len(topics) > 0 else None,
                        to_hex(topics[1]) if len(topics) > 1 else None,
                        to_hex(topics[2]) if len(topics) > 2 else None,
                        to_hex(topics[3]) if len(topics) > 3 else None,
                        to_hex(log['data']) if isinstance(log['data'], bytes) else log['data'],
                        log['logIndex']
                    ))
                
                input_data = tx['input'].hex() if isinstance(tx['input'], bytes) else tx['input']
                method_id = input_data[:10] if input_data and len(input_data) >= 10 else input_data

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
                    input_data,
                    method_id,
                    tx['nonce'],
                    tx['transactionIndex'],
                    receipt['status']
                ))
            
            if tx_data:
                extras.execute_values(
                    cur,
                    "INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas, input_data, method_id, nonce, transaction_index, status) VALUES %s ON CONFLICT (tx_hash) DO NOTHING",
                    tx_data
                )
            
            if log_data:
                extras.execute_values(
                    cur,
                    "INSERT INTO transaction_logs (tx_hash, address, topic0, topic1, topic2, topic3, data, log_index) VALUES %s ON CONFLICT (tx_hash, log_index) DO NOTHING",
                    log_data
                )
        
        conn.commit()
        logger.info(f"Successfully loaded block {block_number} with {len(block['transactions'])} transactions.")
        return True
    except Exception as e:
        logger.error(f"Error loading block {block_number}: {e}")
        conn.rollback()
        return False

def main(start_block, end_block):
    if not RPC_URL:
        logger.error("RPC_URL not found in environment variables.")
        return

    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        logger.error("Failed to connect to Base RPC.")
        return
    
    # Initialize secondary RPC for receipts if available
    if ALCHEMY_RPC_URL:
        w3_receipts = Web3(Web3.HTTPProvider(ALCHEMY_RPC_URL))
        if not w3_receipts.is_connected():
            logger.warning("Failed to connect to Alchemy RPC, falling back to primary RPC for receipts.")
            w3_receipts = w3
        else:
            logger.info("Connected to Alchemy RPC for transaction receipts.")
    else:
        w3_receipts = w3
    
    current_block = retry_rpc_call(lambda: w3.eth.block_number)
    time.sleep(RPC_DELAY)
    logger.info(f"Connected to Base RPC. Current block: {current_block}")
    
    conn = get_db_connection()
    init_db(conn)
    
    for block_num in range(start_block, end_block + 1):
        fetch_and_store_block(w3, w3_receipts, conn, block_num)
    
    conn.close()
    logger.info("Finished loading blocks.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python load_transactions.py <start_block> <end_block>")
        sys.exit(1)
        
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    main(start, end)
