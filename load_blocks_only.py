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
    minconn=5,
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
            self.clients.append({
                'w3': Web3(Web3.HTTPProvider(url)),
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
    try:
        w3 = rpc_manager.get_next_client(required_block=block_number)
        # Retry logic for individual block fetch
        block = None
        for attempt in range(5):
            try:
                block = w3.eth.get_block(block_number, full_transactions=True)
                # If successful, we can update the known latest block for this RPC
                rpc_manager.update_latest_block(w3, block['number'])
                break
            except Exception as e:
                error_msg = str(e).lower()
                
                # Specific handling for "block not found" (lagging node)
                if "not found" in error_msg or "-32014" in error_msg:
                    logger.warning(f"Block {block_number} not found on {w3.provider.endpoint_uri}. Node might be lagging.")
                    # If the node reports its own latest block, we could use that, 
                    # but for now we just try a different RPC immediately.
                
                # Specific handling for 401 Unauthorized or Rate Limits
                elif "401" in error_msg or "402" in error_msg or "429" in error_msg or "unauthorized" in error_msg or "rate limit" in error_msg:
                    rpc_manager.mark_unhealthy(w3, reason="Auth/RateLimit", duration=600)
                
                else:
                    logger.warning(f"Attempt {attempt+1} failed for block {block_number} on {w3.provider.endpoint_uri}: {e}")
                
                time.sleep(0.5) # Fast retry
                w3 = rpc_manager.get_next_client(required_block=block_number)

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

        conn = db_pool.getconn()
        with conn.cursor() as cur:
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

def main():
    # Get latest block from network
    w3 = rpc_manager.get_next_client()
    latest_block = w3.eth.block_number
    logger.info(f"Latest network block: {latest_block}")
    
    # We'll go back 15.7 million blocks (roughly a year)
    # or stop if the user interrupts.
    target_start_block = latest_block
    target_end_block = max(0, latest_block - 15700000)
    
    logger.info(f"Starting backward scrape from {target_start_block} to {target_end_block} using {MAX_WORKERS} workers.")
    
    blocks_processed = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Process in chunks backwards
        for i in range(target_start_block, target_end_block - 1, -BLOCK_BATCH_SIZE):
            batch_end = max(target_end_block, i - BLOCK_BATCH_SIZE + 1)
            current_batch_range = range(i, batch_end - 1, -1)
            
            # Check which blocks in this batch already exist
            existing = get_existing_blocks(i, batch_end)
            to_fetch = [b for b in current_batch_range if b not in existing]
            
            if not to_fetch:
                continue

            future_to_block = {executor.submit(fetch_and_store_block, b): b for b in to_fetch}
            
            for future in as_completed(future_to_block):
                block_num = future_to_block[future]
                try:
                    success = future.result()
                    if success:
                        blocks_processed += 1
                        if blocks_processed % 100 == 0:
                            logger.info(f"Progress: {blocks_processed} new blocks stored.")
                except Exception as exc:
                    logger.error(f"Block {block_num} generated an exception: {exc}")

    logger.info(f"Finished. Successfully processed {blocks_processed} new blocks.")

if __name__ == "__main__":
    main()
