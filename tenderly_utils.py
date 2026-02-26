import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

TENDERLY_RPC_URL = os.getenv("TENDERLY_RPC_URL")

def trace_transaction(tx_hash):
    """Traces a transaction using Tenderly's trace_transaction RPC method."""
    if not TENDERLY_RPC_URL:
        return {"error": "TENDERLY_RPC_URL not found in .env"}
    
    # Ensure 0x prefix
    clean_hash = tx_hash if tx_hash.startswith("0x") else "0x" + tx_hash

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tenderly_traceTransaction",
        "params": [clean_hash]
    }
    
    try:
        response = requests.post(TENDERLY_RPC_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            return {"error": f"RPC Error: {data['error'].get('message', 'Unknown error')}"}
        
        return data.get("result", {})
    except Exception as e:
        return {"error": f"Request Error: {str(e)}"}
