import json
import tenderly_utils

# Use one of the sample TXs that failed classification
tx_hash = "0x81433b83d2bf6e77d496af8585bfd5bd325bdf5a731928c1975347c254d91309"

print(f"Tracing {tx_hash}...")
trace = tenderly_utils.trace_transaction(tx_hash)

if "error" in trace:
    print(f"Error: {trace['error']}")
else:
    # Save to file for inspection
    with open("trace_dump.json", "w") as f:
        json.dump(trace, f, indent=2)
    print("Trace saved to trace_dump.json")
