from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Dict, Any
import db_utils
import tenderly_utils
import dune_utils

class ClassificationStrategy(ABC):
    """Abstract base class for classification strategies."""
    
    @abstractmethod
    def classify(self, trace: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """
        Analyze the trace and return a (tag, description) tuple if a match is found.
        Return None if no match.
        """
        pass

class ArbitrageStrategy(ClassificationStrategy):
    """Classifies as Arbitrage if the transaction checks multiple pools (e.g., slot0 calls)."""
    
    def classify(self, trace: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        trace_steps = trace.get("trace", [])
        tx_to = (trace.get("transaction_to") or "").lower() if trace.get("transaction_to") else None
        if not trace_steps:
            return None
        print(f"[Arb] trace_steps={len(trace_steps)} tx_to={tx_to}")
            
        slot0_pools = set()
        get_reserves_pools = set()
        
        for step in trace_steps:
            step_input = step.get("input", "")
            if not step_input:
                continue
                
            if not step_input.startswith("0x"):
                step_input = "0x" + step_input
                
            to_address = step.get("to")
            if not to_address:
                continue
                
            # Check for Uniswap V3 slot0()
            if step.get("method") == "slot0":
                slot0_pools.add(to_address.lower())
                
            # Check for Uniswap V2 getReserves()
            elif step.get("method") == "getReserves":
                get_reserves_pools.add(to_address.lower())
        
        v3_count = len(slot0_pools)
        v2_count = len(get_reserves_pools)
        print(f"[Arb] v3_pools={v3_count} v2_pools={v2_count}")
        heur_positive = v3_count >= 2 or v2_count >= 2 or (v3_count + v2_count) >= 2
        if heur_positive:
            candidates = []
            if tx_to:
                candidates.append(tx_to)
            for addr in list(slot0_pools)[:5]:
                if addr not in candidates:
                    candidates.append(addr)
            for addr in list(get_reserves_pools)[:5]:
                if addr not in candidates:
                    candidates.append(addr)
            print(f"[Arb] candidates={candidates}")
            for addr in candidates[:5]:
                count = dune_utils.get_multi_swap_tx_count(addr, blockchain="base")
                print(f"[Arb] dune multi-swap count for {addr} -> {count}")
                if count is None:
                    continue
                if count > 0:
                    if v3_count >= 2 and v2_count == 0:
                        return "MEV Bot: Arbitrage", f"slot0 checks on {v3_count} pools; Dune shows {count} multi-swaps for {addr}"
                    if v2_count >= 2 and v3_count == 0:
                        return "MEV Bot: Arbitrage", f"getReserves checks on {v2_count} pools; Dune shows {count} multi-swaps for {addr}"
                    return "MEV Bot: Arbitrage", f"Checked {v3_count} V3 and {v2_count} V2 pools; Dune shows {count} multi-swaps for {addr}"
            if v3_count >= 2 and v2_count == 0:
                return "MEV Bot: Arbitrage", f"Checked {v3_count} V3 type pools via slot0()"
            if v2_count >= 2 and v3_count == 0:
                return "MEV Bot: Arbitrage", f"Checked {v2_count} V2 type pools via getReserves()"
            return "MEV Bot: Arbitrage", f"Checked {v3_count} V3 and {v2_count} V2 pools"
            
        return None
    
class OracleLeveragingArbitrageStrategy(ClassificationStrategy):
    """Classifies as Oracle Leveraging Arbitrage if the transaction checks oracles for latestanswer."""
    
    def classify(self, trace: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        trace_steps = trace.get("trace", [])
        tx_to = (trace.get("transaction_to") or "").lower() if trace.get("transaction_to") else None
        if not trace_steps:
            return None
        
        top_input = None
        for s in trace_steps:
            # Record only the first non-empty input as top-level for matching
            si = s.get("input")
            if isinstance(si, str) and si and si != "0x":
                top_input = si if si.startswith("0x") else "0x" + si
                top_input = top_input.lower()
                break
        
        saw_oracle_read = False
        for step in trace_steps:
            step_input = step.get("input", "")
            if not step_input:
                continue
            if not step_input.startswith("0x"):
                step_input = "0x" + step_input
            if step.get("method") == "latestAnswer":
                saw_oracle_read = True
                break
        
        if not saw_oracle_read:
            return None
        
        candidates = []
        if top_input:
            seen = set()
            for s in trace_steps:
                si = s.get("input")
                if not isinstance(si, str):
                    continue
                norm = si if si.startswith("0x") else "0x" + si
                norm = norm.lower()
                if norm == top_input:
                    to_addr = s.get("to")
                    if isinstance(to_addr, str):
                        cand = to_addr.lower()
                        if cand and cand != tx_to and cand not in seen:
                            seen.add(cand)
                            candidates.append(cand)
        
        if tx_to and tx_to not in candidates:
            candidates.insert(0, tx_to)
        
        print(f"Oracle Leveraging Arbitrage candidates: {candidates}")
        for addr in candidates[:5]:
            count = dune_utils.get_multi_swap_tx_count(addr, blockchain="base")
            print(f"[OracleArb] dune multi-swap count for {addr} -> {count}")
            if count is None:
                continue
            if count > 0:
                return "MEV Bot: Oracle Leveraging Arbitrage", f"Oracle read observed; {addr} shows {count} multi-swap txs on Dune"
            
        return None

class OracleLeveragingLiquidationStrategy(ClassificationStrategy):
    def classify(self, trace: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        trace_steps = trace.get("trace", [])
        tx_to = (trace.get("transaction_to") or "").lower() if trace.get("transaction_to") else None
        if not trace_steps:
            return None
        top_input = None
        for s in trace_steps:
            si = s.get("input")
            if isinstance(si, str) and si and si != "0x":
                top_input = si if si.startswith("0x") else "0x" + si
                top_input = top_input.lower()
                break
        saw_oracle_read = False
        for step in trace_steps:
            step_input = step.get("input", "")
            if not step_input:
                continue
            if not step_input.startswith("0x"):
                step_input = "0x" + step_input
            if step.get("method") == "latestAnswer":
                saw_oracle_read = True
                break
        if not saw_oracle_read:
            return None
        candidates = []
        if top_input:
            seen = set()
            print("[OracleLiq] top_input set; scanning same-input forwards")
            for s in trace_steps:
                si = s.get("input")
                if not isinstance(si, str):
                    continue
                norm = si if si.startswith("0x") else "0x" + si
                norm = norm.lower()
                if norm == top_input:
                    to_addr = s.get("to")
                    if isinstance(to_addr, str):
                        cand = to_addr.lower()
                        if cand and cand != tx_to and cand not in seen:
                            seen.add(cand)
                            candidates.append(cand)
        if tx_to and tx_to not in candidates:
            candidates.insert(0, tx_to)
        print(f"[OracleLiq] candidates={candidates}")
        for addr in candidates[:5]:
            count = dune_utils.get_liquidation_tx_count(addr, blockchain="base")
            print(f"[OracleLiq] dune liquidation count for {addr} -> {count}")
            if count is None:
                continue
            if count > 0:
                return "MEV Bot: Oracle Leveraging Liquidation", f"Oracle read observed; {addr} shows {count} liquidation txs on Dune"
        return None

class LiquidationStrategy(ClassificationStrategy):
    def classify(self, trace: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        tx_to = (trace.get("transaction_to") or "").lower() if trace.get("transaction_to") else None
        if not tx_to:
            return None
        print(f"[Liq] checking tx.to={tx_to}")
        count = dune_utils.get_liquidation_tx_count(tx_to, blockchain="base")
        print(f"[Liq] dune liquidation count for {tx_to} -> {count}")
        if count is None:
            return None
        if count > 0:
            return "MEV Bot: Liquidation", f"{tx_to} has {count} liquidation txs on Dune"
        return None

class MethodClassifier:
    def __init__(self):
        self.strategies: List[ClassificationStrategy] = [
            OracleLeveragingLiquidationStrategy(),
            OracleLeveragingArbitrageStrategy(),
            LiquidationStrategy(),
            ArbitrageStrategy(),
            # LogSignatureStrategy(), # Disabled as requested
            # Add more strategies here
        ]
        
    def classify_method(self, method_id: str, tx_hash: str) -> Tuple[str, str]:
        """
        Fetches trace and applies strategies to classify the method.
        Returns (tag, description).
        """
        print(f"Tracing {tx_hash} for method {method_id}...")
        trace = tenderly_utils.trace_transaction(tx_hash)
        if "error" in trace:
            return "Error", trace["error"]
        
        tx_to = None
        tx_value = None
        input_data = None
        try:
            conn = db_utils.get_db_connection()
            cur = conn.cursor()
            alt_hash = tx_hash[2:] if tx_hash.startswith("0x") else "0x" + tx_hash
            cur.execute(
                "SELECT to_address, value, input_data FROM transactions WHERE tx_hash = %s OR tx_hash = %s LIMIT 1",
                (tx_hash, alt_hash,)
            )
            row = cur.fetchone()
            print(f"[Classify] DB fetch for {tx_hash}: row_present={bool(row)}")
            if row:
                tx_to, tx_value, input_data = row[0], row[1], row[2]
            if tx_to:
                trace["transaction_to"] = tx_to
                print(f"[Classify] DB tx_to={tx_to} value={tx_value} input_len={(len(input_data) if isinstance(input_data,str) else None)}")
            cur.close()
            conn.close()
        except Exception:
            pass
        
        # Fallback injection of tx.to from the top-level trace if DB lookup failed
        if not tx_to:
            try:
                steps = trace.get("trace", [])
                print(f"[Classify] DB missing tx_to; steps={len(steps)}")
                root = None
                for s in steps:
                    addr_path = s.get("traceAddress", [])
                    if isinstance(addr_path, list) and len(addr_path) == 0:
                        root = s
                        break
                if not root and steps:
                    root = steps[0]
                if root:
                    candidate_to = root.get("to")
                    if isinstance(candidate_to, str) and candidate_to:
                        tx_to = candidate_to
                        trace["transaction_to"] = tx_to
                        print(f"[Classify] Fallback tx_to from trace root: {tx_to}")
            except Exception:
                pass
        
        selector = None
        if input_data and isinstance(input_data, str):
            s = input_data if input_data.startswith("0x") else "0x" + input_data
            selector = s[:10].lower() if len(s) >= 10 else s.lower()
        elif isinstance(method_id, str) and method_id:
            selector = method_id.lower()
        print(f"[Classify] selector={selector} tx_to={tx_to}")
        
        # Only mark as deployment if we can detect CREATE/CREATE2 in trace or there is truly no tx.to anywhere
        if tx_to is None:
            saw_create = any(isinstance(s.get("type"), str) and s.get("type") in ("CREATE", "CREATE2") for s in trace.get("trace", []))
            if saw_create:
                return "Contract Deployment", "CREATE detected in trace"
        
        if (not input_data or input_data in ("0x", "")) and tx_value and float(tx_value) > 0:
            return "Eth Transfer", "non-zero value and empty calldata"
        
        erc_tags = {
            "0x095ea7b3": ("ERC20 Approval", "approve"),
            "0xa9059cbb": ("ERC20 Transfer", "transfer"),
            "0x23b872dd": ("ERC721 Transfer", "transferFrom"),
            "0x42842e0e": ("ERC721 Transfer", "safeTransferFrom"),
            "0xb88d4fde": ("ERC721 Transfer", "safeTransferFrom"),
        }
        if selector in erc_tags:
            tag, reason = erc_tags[selector]
            return tag, reason
        
        v2_swaps = {
            "0x38ed1739",
            "0x8803dbee",
            "0x7ff36ab5",
            "0x4a25d94a",
            "0x18cbafe5",
            "0xfb3bdb41",
        }
        if selector in v2_swaps:
            return "Uniswap V2 Swap", "router swap selector"
        
        if selector in {"0x04e45aaf"}:
            return "Uniswap V3 Swap", "exactInputSingle"
            
        for strategy in self.strategies:
            result = strategy.classify(trace)
            if result:
                return result
                
        return "Unknown", "No matching strategy found"

    def fetch_untagged_methods(self, limit=10):
        conn = db_utils.get_db_connection()
        cur = conn.cursor()
        try:
            db_utils.ensure_method_frequencies_view()
        except Exception:
            pass
        query = """
        SELECT 
            method_id,
            usage_count
        FROM method_frequencies
        ORDER BY usage_count DESC
        LIMIT 1000;
        """
        cur.execute(query)
        all_methods = cur.fetchall()
        
        cur.execute("SELECT method_id FROM method_tags")
        tagged_ids = {row[0] for row in cur.fetchall()}
        
        untagged_top = []
        for mid, count in all_methods:
            if mid not in tagged_ids:
                untagged_top.append((mid, count))
            if len(untagged_top) >= limit:
                break
        
        results = []
        for mid, count in untagged_top:
            cur.execute("""
                SELECT tx_hash
                FROM transactions 
                WHERE method_id = %s 
                LIMIT 1
            """, (mid,))
            sample = cur.fetchone()
            if sample:
                results.append({
                    "method_id": mid,
                    "usage_count": count,
                    "sample_tx_hash": sample[0],
                })
                
        cur.close()
        conn.close()
        return results

    def save_tag(self, method_id, tag_name, description):
        conn = db_utils.get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO method_tags (method_id, tag_name, description) VALUES (%s, %s, %s) ON CONFLICT (method_id) DO UPDATE SET tag_name = EXCLUDED.tag_name, description = EXCLUDED.description",
            (method_id, tag_name, description)
        )
        conn.commit()
        cur.close()
        conn.close()
