import os
import requests
import json
import time
from dotenv import load_dotenv

load_dotenv()

TENDERLY_RPC_URL = os.getenv("TENDERLY_RPC_URL")

def trace_transaction(tx_hash, max_retries=5, initial_delay=1.0):
    """Traces a transaction using Tenderly's trace_transaction RPC method with retries."""
    if not TENDERLY_RPC_URL:
        return {"error": "TENDERLY_RPC_URL not found in .env"}
    
    clean_hash = tx_hash if tx_hash.startswith("0x") else "0x" + tx_hash
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tenderly_traceTransaction",
        "params": [clean_hash]
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(TENDERLY_RPC_URL, json=payload)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            
            if "error" in data:
                # Tenderly might return an error in the JSON response even with 200 status
                error_message = data['error'].get('message', 'Unknown error')
                if "rate limit" in error_message.lower() or "too many requests" in error_message.lower():
                    if attempt < max_retries - 1:
                        delay = initial_delay * (2 ** attempt)
                        print(f"Rate limit hit (Tenderly JSON error). Retrying in {delay:.2f} seconds...")
                        time.sleep(delay)
                        continue
                return {"error": f"RPC Error: {error_message}"}
            
            return data.get("result", {})
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    print(f"Rate limit hit (HTTP 429). Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                    continue
                return {"error": f"Request Error: {str(e)} after {max_retries} attempts"}
            else:
                return {"error": f"Request Error: {str(e)}"}
        except Exception as e:
            return {"error": f"Request Error: {str(e)}"}
            
    return {"error": f"Failed to trace transaction after {max_retries} attempts due to rate limiting."}
