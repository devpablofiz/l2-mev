import argparse
import sys
import db_utils
from method_classifier import MethodClassifier

def main():
    parser = argparse.ArgumentParser(description="Run MEV Method Classifier")
    parser.add_argument("--limit", type=int, default=10, help="Number of methods to classify")
    parser.add_argument("--auto-save", action="store_true", help="Automatically save high-confidence tags")
    parser.add_argument("--refresh-view", action="store_true", help="Refresh method frequencies before running")
    parser.add_argument("--tx", type=str, help="Classify a specific transaction hash (0x...)")
    args = parser.parse_args()

    if args.refresh_view:
        print("Refreshing method frequencies view...")
        try:
            db_utils.ensure_method_frequencies_view()
            db_utils.refresh_method_frequencies(concurrently=True)
            print("View refreshed.")
        except Exception as e:
            print(f"Could not refresh view: {e}")

    # Single-TX classification mode
    if args.tx:
        tx_hash = args.tx
        try:
            conn = db_utils.get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT method_id 
                FROM transactions 
                WHERE tx_hash = %s OR tx_hash = %s
                LIMIT 1
            """, (tx_hash, tx_hash[2:] if tx_hash.startswith("0x") else "0x"+tx_hash))
            row = cur.fetchone()
            if not row:
                cur.execute("""
                    SELECT input_data 
                    FROM transactions 
                    WHERE tx_hash = %s OR tx_hash = %s
                    LIMIT 1
                """, (tx_hash, tx_hash[2:] if tx_hash.startswith("0x") else "0x"+tx_hash))
                r2 = cur.fetchone()
                if r2 and r2[0]:
                    input_data = r2[0]
                    if input_data and not input_data.startswith("0x"):
                        input_data = "0x" + input_data
                    method_id = input_data[:10] if len(input_data) >= 10 else input_data
                else:
                    method_id = "unknown"
            else:
                method_id = row[0]
            cur.close()
            conn.close()
        except Exception as e:
            print(f"DB error while fetching method_id: {e}")
            method_id = "unknown"
        
        classifier = MethodClassifier()
        print(f"Classifying TX: {tx_hash} (method_id: {method_id})")
        tag, description = classifier.classify_method(method_id, tx_hash)
        print(f"Result: {tag} - {description}")
        return

    classifier = MethodClassifier()
    
    print(f"Fetching top {args.limit} untagged methods...")
    methods = classifier.fetch_untagged_methods(limit=args.limit)
    
    if not methods:
        print("No untagged methods found.")
        return

    print(f"Found {len(methods)} methods. Starting classification...\n")
    
    for method in methods:
        mid = method["method_id"]
        count = method["usage_count"]
        first_tx_hash = method["sample_tx_hash"]
        
        print(f"Analyzing Method: {mid} (Used {count} times)")
        print(f"Sample TX: {first_tx_hash}")
        
        # Try the first sample; if unknown/error, try up to 9 more recent txs with distinct to_address (total <= 10)
        tag, description = classifier.classify_method(mid, first_tx_hash)
        classified_tx = first_tx_hash
        if not tag or tag in ("Unknown", "Error"):
            try:
                conn = db_utils.get_db_connection()
                cur = conn.cursor()
                print("Fetching retry candidates (latest first)...")
                cur.execute("""
                    SELECT 
                        tx_hash,
                        to_address,
                        block_number
                    FROM transactions
                    WHERE method_id = %s
                    ORDER BY block_number DESC
                    LIMIT 30
                """, (mid,))
                rows = cur.fetchall() or []
                cur.close()
                conn.close()
            except Exception as e:
                rows = []
                print(f"Warning: could not fetch additional samples for {mid}: {e}")
            
            tried_hashes = {first_tx_hash}
            seen_to = set()
            attempts = 0
            for tx_hash, to_addr, _bn in rows:
                if attempts >= 9:
                    break
                if tx_hash in tried_hashes:
                    continue
                to_norm = (to_addr or "").lower()
                if to_norm and to_norm in seen_to:
                    continue
                print(f" - Retrying with example TX: {tx_hash} (to: {to_addr})")
                tag, description = classifier.classify_method(mid, tx_hash)
                classified_tx = tx_hash
                tried_hashes.add(tx_hash)
                if to_norm:
                    seen_to.add(to_norm)
                attempts += 1
                if tag and tag not in ("Unknown", "Error"):
                    break
        
        if tag and tag != "Unknown" and tag != "Error":
            print(f"✅ SUGGESTED TAG: {tag}")
            print(f"   Reason: {description}")
            print(f"   Example TX used: {classified_tx}")
            
            if args.auto_save:
                print("   Saving tag to database...")
                classifier.save_tag(mid, tag, description)
                print("   Saved.")
        else:
            print(f"❌ Could not classify after up to 10 samples. Last result: {tag} ({description})")
            
        print("-" * 50)

if __name__ == "__main__":
    main()
