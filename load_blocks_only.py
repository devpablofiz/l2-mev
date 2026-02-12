import os
import sys
import logging
import psycopg2
import time
import threading
from psycopg2 import extras, pool
from web3 import Web3
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# DB Config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "l2_mev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# RPC Config - Expects a comma-separated list of URLs
RPC_URLS = os.getenv("RPC_URLS", os.getenv("RPC_URL", "")).split(",")
RPC_URLS = [url.strip() for url in RPC_URLS if url.strip()]

# Performance Config
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10")) # Parallel threads
BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "100")) # Blocks per thread batch

# Initialize Connection Pool
db_pool = pool.ThreadedConnectionPool(
    minconn=MAX_WORKERS,
    maxconn=MAX_WORKERS + 5,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

class RPCManager:
    def __init__(self, urls):
        self.urls = urls
        self.clients = []
        for url in urls:
            # Add a default timeout of 30s to prevent hanging indefinitely
            self.clients.append({
                'w3': Web3(Web3.HTTPProvider(url, request_kwargs={'timeout': 30})),
                'url': url,
                'unhealthy_until': 0,
                'latest_block': 0
            })
        self.lock = threading.Lock()
        self.index = 0
        if not self.clients:
            logger.error("No RPC URLs provided!")
            sys.exit(1)
        logger.info(f"Initialized RPCManager with {len(self.clients)} endpoints.")

    def get_next_client(self, required_block=None):
        """Round-robin RPC selection with health and lag checks."""
        with self.lock:
            start_index = self.index
            now = time.time()
            
            while True:
                client_data = self.clients[self.index]
                self.index = (self.index + 1) % len(self.clients)
                
                # Check if client is in cooldown
                if client_data['unhealthy_until'] > now:
                    if self.index == start_index: # We've checked all clients
                        # All clients are unhealthy? Pick the least unhealthy or wait
                        break
                    continue
                
                # Check if client is known to be lagging behind the required block
                if required_block and client_data['latest_block'] > 0 and client_data['latest_block'] < required_block:
                    if self.index == start_index:
                        break
                    continue
                
                return client_data['w3']
            
            # Fallback: if all are filtered out, just return the next one anyway
            return self.clients[self.index]['w3']

    def mark_unhealthy(self, w3, reason="unknown", duration=300):
        """Mark an RPC as unhealthy for a duration (default 5 mins)."""
        url = w3.provider.endpoint_uri
        with self.lock:
            for client in self.clients:
                if client['url'] == url:
                    client['unhealthy_until'] = time.time() + duration
                    logger.warning(f"RPC {url} marked unhealthy for {duration}s. Reason: {reason}")
                    break

    def update_latest_block(self, w3, block_number):
        """Update the known latest block for an RPC to avoid future lag errors."""
        url = w3.provider.endpoint_uri
        with self.lock:
            for client in self.clients:
                if client['url'] == url:
                    client['latest_block'] = max(client['latest_block'], block_number)
                    break

rpc_manager = RPCManager(RPC_URLS)

def fetch_and_store_block(block_number):
    """Fetches a block and its transactions, then stores them in DB."""
    conn = None
    start_time = time.time()
    try:
        w3 = rpc_manager.get_next_client(required_block=block_number)
        rpc_url = w3.provider.endpoint_uri
        # Retry logic for individual block fetch
        block = None
        for attempt in range(5):
            try:
                fetch_start = time.time()
                block = w3.eth.get_block(block_number, full_transactions=True)
                fetch_duration = time.time() - fetch_start
                
                # If successful, we can update the known latest block for this RPC
                rpc_manager.update_latest_block(w3, block['number'])
                
                # logger.info(f"Fetched block {block_number} from {rpc_url} in {fetch_duration:.2f}s")
                break
            except Exception as e:
                error_msg = str(e).lower()
                
                # Specific handling for timeout
                if "timeout" in error_msg:
                    logger.warning(f"Timeout fetching block {block_number} from {rpc_url}. Marking unhealthy.")
                    rpc_manager.mark_unhealthy(w3, reason="Timeout", duration=120)
                
                # Specific handling for "block not found" (lagging node)
                elif "not found" in error_msg or "-32014" in error_msg:
                    logger.warning(f"Block {block_number} not found on {rpc_url}. Node might be lagging.")
                
                # Specific handling for 401 Unauthorized or Rate Limits
                elif "401" in error_msg or "402" in error_msg or "429" in error_msg or "unauthorized" in error_msg or "rate limit" in error_msg:
                    rpc_manager.mark_unhealthy(w3, reason="Auth/RateLimit", duration=600)
                
                else:
                    logger.warning(f"Attempt {attempt+1} failed for block {block_number} on {rpc_url}: {e}")
                
                time.sleep(0.5) # Fast retry
                w3 = rpc_manager.get_next_client(required_block=block_number)
                rpc_url = w3.provider.endpoint_uri

        if not block:
            logger.error(f"Failed to fetch block {block_number} after 5 attempts.")
            return False

        timestamp = datetime.fromtimestamp(block['timestamp'])
        
        tx_data = []
        for tx in block['transactions']:
            try:
                # Reverting to strict access to catch incomplete data
                input_data = tx['input']
                if isinstance(input_data, bytes):
                    input_data = input_data.hex()
                elif input_data is None:
                    input_data = ''
                    
                method_id = input_data[:10] if input_data and len(input_data) >= 10 else input_data
                
                tx_hash = tx['hash']
                if isinstance(tx_hash, bytes):
                    tx_hash = tx_hash.hex()
                
                # These fields are required for integrity
                nonce = tx['nonce']
                from_addr = tx['from']
                to_addr = tx['to']
                value = float(Web3.from_wei(tx['value'], 'ether'))
                
                tx_data.append((
                    tx_hash,
                    block['number'],
                    from_addr,
                    to_addr,
                    value,
                    tx.get('gas'),
                    tx.get('gasPrice'),
                    tx.get('maxFeePerGas'),
                    tx.get('maxPriorityFeePerGas'),
                    input_data,
                    method_id,
                    nonce,
                    tx.get('transactionIndex')
                ))
            except KeyError as e:
                # LOG THE OFFENDING RPC
                offending_url = w3.provider.endpoint_uri
                logger.error(f"INCOMPLETE DATA detected on RPC {offending_url} for block {block_number}: Missing field {e}")
                # Re-raise to trigger the retry/switch logic in the outer loop
                raise Exception(f"Incomplete transaction data (missing {e}) from {offending_url}")

        db_conn_start = time.time()
        conn = db_pool.getconn()
        db_conn_duration = time.time() - db_conn_start
        
        db_exec_start = time.time()
        with conn.cursor() as cur:
            # Performance optimization: Disable synchronous commit for this session.
            # This significantly speeds up inserts by not waiting for disk flush.
            cur.execute("SET synchronous_commit TO OFF")
            
            # Insert block
            cur.execute(
                "INSERT INTO blocks (block_number, block_hash, timestamp) VALUES (%s, %s, %s) ON CONFLICT (block_number) DO NOTHING",
                (block['number'], block['hash'].hex(), timestamp)
            )
            
            # Insert transactions
            if tx_data:
                extras.execute_values(
                    cur,
                    "INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas, input_data, method_id, nonce, transaction_index) VALUES %s ON CONFLICT (tx_hash) DO NOTHING",
                    tx_data
                )
        
        conn.commit()
        db_exec_duration = time.time() - db_exec_start
        
        total_duration = time.time() - start_time
        logger.info(f"Stored block {block_number} ({len(tx_data)} txs) from {rpc_url} | Fetch: {fetch_duration:.2f}s | DB Conn: {db_conn_duration:.2f}s | DB Exec: {db_exec_duration:.2f}s | Total: {total_duration:.2f}s")
        
        return True
    except Exception as e:
        logger.error(f"Error processing block {block_number}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            db_pool.putconn(conn)

def get_existing_blocks(start_block, end_block):
    """Returns a set of block numbers already in the DB within a range."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT block_number FROM blocks WHERE block_number BETWEEN %s AND %s", (min(start_block, end_block), max(start_block, end_block)))
            return {row[0] for row in cur.fetchall()}
    finally:
        db_pool.putconn(conn)

def get_db_block_range():
    """Returns the (min, max) block numbers in the database."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(block_number), MAX(block_number) FROM blocks")
            return cur.fetchone()
    finally:
        db_pool.putconn(conn)

def process_block_range(executor, start_block, end_block):
    """Processes blocks in the given range (inclusive, descending)."""
    blocks_processed = 0
    total_blocks_to_check = abs(start_block - end_block) + 1
    logger.info(f"Processing range {start_block} to {end_block} ({total_blocks_to_check} blocks)")
    
    # Process in chunks backwards
    for i in range(start_block, end_block - 1, -BLOCK_BATCH_SIZE):
        batch_end = max(end_block, i - BLOCK_BATCH_SIZE + 1)
        current_batch_range = range(i, batch_end - 1, -1)
        
        # Check which blocks in this batch already exist
        existing = get_existing_blocks(i, batch_end)
        to_fetch = [b for b in current_batch_range if b not in existing]
        
        if not to_fetch:
            # logger.debug(f"Batch {i} to {batch_end} already fully in DB.")
            continue

        logger.info(f"Fetching {len(to_fetch)} missing blocks in batch {i} to {batch_end}")
        future_to_block = {executor.submit(fetch_and_store_block, b): b for b in to_fetch}
        
        batch_processed = 0
        for future in as_completed(future_to_block):
            block_num = future_to_block[future]
            try:
                success = future.result()
                if success:
                    blocks_processed += 1
                    batch_processed += 1
                    if blocks_processed % 10 == 0: # More frequent logging
                        logger.info(f"Progress: {blocks_processed} new blocks stored in this range.")
            except Exception as exc:
                logger.error(f"Block {block_num} generated an exception: {exc}")
        
        logger.info(f"Batch {i} to {batch_end} complete. Processed {batch_processed} blocks.")
    return blocks_processed

def main():
    # Get latest block from network
    w3 = rpc_manager.get_next_client()
    latest_network_block = w3.eth.block_number
    logger.info(f"Latest network block: {latest_network_block}")
    
    # Get DB range
    min_db_block, max_db_block = get_db_block_range()
    logger.info(f"DB block range: {min_db_block} to {max_db_block}")

    total_new_blocks = 0
    target_end_block = max(0, latest_network_block - 15700000)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Phase 1: Fill new blocks (from latest network down to max DB block)
        if max_db_block is not None:
            p1_start = latest_network_block
            p1_end = max_db_block + 1
            if p1_start >= p1_end:
                logger.info(f"Phase 1: Filling new blocks from {p1_start} down to {p1_end}")
                total_new_blocks += process_block_range(executor, p1_start, p1_end)
        else:
            # If DB is empty, just do one big range
            logger.info(f"Database is empty. Starting full scrape from {latest_network_block} to {target_end_block}")
            total_new_blocks += process_block_range(executor, latest_network_block, target_end_block)
            logger.info(f"Finished. Successfully processed {total_new_blocks} new blocks.")
            return

        # Phase 2: Backfill from min DB block down to target
        if min_db_block is not None:
            p2_start = min_db_block - 1
            p2_end = target_end_block
            if p2_start >= p2_end:
                logger.info(f"Phase 2: Resuming backfill from {p2_start} down to {p2_end}")
                total_new_blocks += process_block_range(executor, p2_start, p2_end)

    logger.info(f"Finished. Successfully processed {total_new_blocks} new blocks.")

if __name__ == "__main__":
    main()
