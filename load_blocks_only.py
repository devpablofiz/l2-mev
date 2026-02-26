import os
import sys
import logging
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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20")) # Parallel threads
BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "200")) # Total blocks to process in one outer loop iteration
RPC_BATCH_SIZE = int(os.getenv("RPC_BATCH_SIZE", "10")) # Blocks per RPC batch request

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

def fetch_and_store_block_batch(block_numbers):
    """Fetches a batch of blocks and their transactions, then stores them in DB."""
    if not block_numbers:
        return 0
        
    conn = None
    try:
        w3 = rpc_manager.get_next_client(required_block=max(block_numbers))
        rpc_url = w3.provider.endpoint_uri
        
        blocks = []
        # Retry logic for the batch
        for attempt in range(5):
            try:
                with w3.batch_requests() as batch:
                    for b_num in block_numbers:
                        batch.add(w3.eth.get_block(b_num, full_transactions=True))
                    blocks = batch.execute()
                
                if blocks and all(blocks):
                    # If successful, update latest block
                    max_fetched = max(b['number'] for b in blocks if b)
                    rpc_manager.update_latest_block(w3, max_fetched)
                    break
            except Exception as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg:
                    rpc_manager.mark_unhealthy(w3, reason="Timeout", duration=120)
                elif "not found" in error_msg or "-32014" in error_msg:
                    logger.warning(f"Some blocks in batch {block_numbers} not found on {rpc_url}.")
                elif any(x in error_msg for x in ["401", "402", "429", "-32003", "unauthorized", "rate limit",
                                                  "too many requests"]):
                    rpc_manager.mark_unhealthy(w3, reason="Auth/RateLimit", duration=10)
                else:
                    logger.warning(f"Attempt {attempt+1} failed for batch starting at {block_numbers[0]} on {rpc_url}: {e}")
                
                if attempt < 4:
                    time.sleep(0.5)
                    w3 = rpc_manager.get_next_client(required_block=max(block_numbers))
                    rpc_url = w3.provider.endpoint_uri
                else:
                    logger.error(f"Failed to fetch batch {block_numbers} after 5 attempts.")
                    return 0

        if not blocks:
            return 0

        all_tx_data = []
        all_block_data = []
        
        for block in blocks:
            if not block: 
                continue
            
            timestamp = datetime.fromtimestamp(block['timestamp'])
            all_block_data.append((block['number'], block['hash'].hex(), timestamp))
            
            for tx in block['transactions']:
                try:
                    input_data = tx['input']
                    if isinstance(input_data, bytes):
                        input_data = input_data.hex()
                    elif input_data is None:
                        input_data = ''
                        
                    method_id = input_data[:10] if input_data and len(input_data) >= 10 else input_data
                    
                    # Truncate input_data to 32 chars MAX
                    if len(input_data) > 32:
                        input_data = input_data[:32]
                    
                    tx_hash = tx['hash']
                    if isinstance(tx_hash, bytes):
                        tx_hash = tx_hash.hex()
                    
                    tx_data = (
                        tx_hash,
                        block['number'],
                        tx['from'],
                        tx['to'],
                        float(Web3.from_wei(tx['value'], 'ether')),
                        tx.get('gas'),
                        tx.get('gasPrice'),
                        tx.get('maxFeePerGas'),
                        tx.get('maxPriorityFeePerGas'),
                        input_data,
                        method_id,
                        tx['nonce'],
                        tx.get('transactionIndex')
                    )
                    all_tx_data.append(tx_data)
                except KeyError as e:
                    logger.error(f"INCOMPLETE DATA on RPC {rpc_url} for block {block['number']}: Missing field {e}")
                    # We might want to skip this block or retry, but for now let's just log and continue
                    continue

        if not all_block_data:
            return 0

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit TO OFF")
                
                # Insert blocks
                extras.execute_values(
                    cur,
                    "INSERT INTO blocks (block_number, block_hash, timestamp) VALUES %s ON CONFLICT (block_number) DO NOTHING",
                    all_block_data
                )
                
                # Insert transactions
                if all_tx_data:
                    extras.execute_values(
                        cur,
                        "INSERT INTO transactions (tx_hash, block_number, from_address, to_address, value, gas_limit, gas_price, max_fee_per_gas, max_priority_fee_per_gas, input_data, method_id, nonce, transaction_index) VALUES %s ON CONFLICT (tx_hash) DO NOTHING",
                        all_tx_data
                    )
            conn.commit()
            return len(all_block_data)
        except Exception as e:
            logger.error(f"DB Error processing batch: {e}")
            conn.rollback()
            return 0
        finally:
            db_pool.putconn(conn)

    except Exception as e:
        logger.error(f"Unexpected error in batch processing: {e}")
        return 0

def fetch_and_store_block(block_number):
    """Fallback/Legacy single block fetcher, now just calls the batch version."""
    return fetch_and_store_block_batch([block_number]) > 0

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
            continue

        # Split to_fetch into smaller chunks for RPC batching
        rpc_chunks = [to_fetch[j:j + RPC_BATCH_SIZE] for j in range(0, len(to_fetch), RPC_BATCH_SIZE)]
        
        future_to_chunk = {executor.submit(fetch_and_store_block_batch, chunk): chunk for chunk in rpc_chunks}
        
        for future in as_completed(future_to_chunk):
            try:
                count = future.result()
                if count > 0:
                    blocks_processed += count
                    if blocks_processed % 100 < count: # Log every 100 blocks
                        logger.info(f"Progress: {blocks_processed} new blocks stored in this range.")
            except Exception as exc:
                chunk = future_to_chunk[future]
                logger.error(f"Batch starting at {chunk[0]} generated an exception: {exc}")
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
