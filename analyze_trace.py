import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

TENDERLY_RPC_URL = os.getenv("TENDERLY_RPC_URL")

# Known Event Signatures
TRANSFER_EVENT_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SWAP_V2_SIG = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V3_SIG = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
SYNC_SIG = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

# Known Addresses (Arbitrum - adjust if needed, but signatures are universal)
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

def trace_transaction(tx_hash):
    if not TENDERLY_RPC_URL:
        print("Error: TENDERLY_RPC_URL not found in .env")
        return None
    
    # Ensure 0x prefix is handled correctly
    if tx_hash.startswith("0x"):
        clean_hash = tx_hash
    else:
        clean_hash = "0x" + tx_hash

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tenderly_traceTransaction",
        "params": [clean_hash]
    }
    
    try:
        print(f"Tracing transaction: {clean_hash}...")
        response = requests.post(TENDERLY_RPC_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            print(f"RPC Error: {data['error'].get('message', 'Unknown error')}")
            return None
            
        return data.get("result", {})
    except Exception as e:
        print(f"Request Error: {e}")
        return None

def analyze_logs(logs):
    print(f"Found {len(logs)} logs.")
    swap_count = 0
    transfer_count = 0
    
    for log in logs:
        topics = log.get("topics", [])
        if not topics:
            continue
            
        sig = topics[0]
        
        if sig == TRANSFER_EVENT_SIG:
            transfer_count += 1
            src = "0x" + topics[1][-40:] if len(topics) > 1 else "?"
            dst = "0x" + topics[2][-40:] if len(topics) > 2 else "?"
            # data usually contains amount
            print(f"  [Transfer] From: {src} To: {dst}")
            
        elif sig == SWAP_V2_SIG:
            swap_count += 1
            print(f"  [Swap V2] at {log.get('address')}")
            
        elif sig == SWAP_V3_SIG:
            swap_count += 1
            print(f"  [Swap V3] at {log.get('address')}")
            
    print(f"Summary: {transfer_count} Transfers, {swap_count} Swaps.")

def analyze_trace_calls(trace):
    # Depending on the structure of trace result. 
    # Usually it's a list of trace objects or a tree.
    # Tenderly returns a list of traces usually?
    # Let's inspect the structure first.
    pass

if __name__ == "__main__":
    # High Gas Price Tx
    tx_hash = "41feb6d7d94489b6e21eb85379baf41811262ccf71a8056c9a947a9cffadbf0c" 
    
    result = trace_transaction(tx_hash)
    
    if result:
        # Check for logs in the trace result
        # The structure might vary. Let's look for 'logs' in the top level or inside trace entries.
        
        # Usually Tenderly trace result has a 'logs' field at top level for the whole tx receipt?
        # Or we might need to iterate through trace steps to find logs.
        
        # Let's print keys to understand structure
        print("Trace keys:", result.keys())
        
        if 'logs' in result:
             print("\n--- Transaction Logs ---")
             analyze_logs(result['logs'])
        else:
             print("No top-level logs found in trace result.")
             
        # Also check for balance changes if available
        if 'assetChanges' in result:
            print("\n--- Asset Changes ---")
            for change in result['assetChanges']:
                 print(change)
        elif 'balanceChanges' in result: # Check alternative key
             print("\n--- Balance Changes ---")
             # Just dump for now
             print(json.dumps(result['balanceChanges'], indent=2))

        # Dump full trace
        if 'trace' in result:
             print(f"\nTrace has {len(result['trace'])} steps.")
             for i, step in enumerate(result['trace']):
                 op = step.get('type')
                 frm = step.get('from')
                 to = step.get('to')
                 val = step.get('value')
                 input_data = step.get('input', '')
                 output_data = step.get('output', '')
                 gas_used = step.get('gasUsed')
                 error = step.get('error')
                 
                 # Extract method signature
                 method_sig = input_data[:10] if input_data and len(input_data) >= 10 else "N/A"
                 
                 print(f"[{i}] {op} {frm} -> {to}")
                 print(f"    Method: {method_sig}")
                 if error:
                     print(f"    ERROR: {error}")
                 if output_data:
                     # Show first few bytes of output
                     short_out = output_data[:66] + "..." if len(output_data) > 66 else output_data
                     print(f"    Output: {short_out}")

    else:
        print("Failed to get trace.")
