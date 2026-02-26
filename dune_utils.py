import os
import re
import time
import requests
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_SQL_ENDPOINT = os.getenv("DUNE_SQL_ENDPOINT", "https://api.dune.com/api/v1/sql")
DUNE_SQL_EXECUTE_ENDPOINT = os.getenv("DUNE_SQL_EXECUTE_ENDPOINT", "https://api.dune.com/api/v1/sql/execute")
DUNE_EXECUTION_RESULT_ENDPOINT = os.getenv("DUNE_EXECUTION_RESULT_ENDPOINT", "https://api.dune.com/api/v1/execution")

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _sanitize_address(addr: str) -> Optional[str]:
    a = addr.lower()
    return a if _ADDR_RE.match(a) else None


def run_sql(sql: str):
    if not DUNE_API_KEY:
        return {"error": "DUNE_API_KEY not set"}
    headers = {"X-Dune-Api-Key": DUNE_API_KEY, "Content-Type": "application/json"}
    try:
        # Step 1: Execute SQL and get execution_id
        r_exec = requests.post(DUNE_SQL_EXECUTE_ENDPOINT, json={"sql": sql}, headers=headers, timeout=30)
        if r_exec.status_code == 200:
            data = r_exec.json()
            execution_id = data.get("execution_id") or data.get("id")
            if execution_id:
                # Step 2: Poll results endpoint until we have rows or terminal state
                # Step 2: Poll results endpoint until we have rows or terminal state
                max_wait_seconds = int(os.getenv("DUNE_MAX_WAIT_SECONDS", "30"))
                poll_interval = float(os.getenv("DUNE_POLL_INTERVAL_SECONDS", "0.75"))
                elapsed = 0.0
                while elapsed < max_wait_seconds:
                    r_res = requests.get(f"{DUNE_EXECUTION_RESULT_ENDPOINT}/{execution_id}/results", headers=headers, timeout=15)
                    if r_res.status_code == 200:
                        d = r_res.json()
                        rows = (d.get("result") or {}).get("rows")
                        if rows is not None:
                            return {"rows": rows}
                        rows = d.get("rows")
                        if rows is not None:
                            return {"rows": rows}
                        st = (d.get("state") or (d.get("execution") or {}).get("state") or "").upper()
                        if "COMPLETED" in st:
                            return {"rows": []}
                        if any(x in st for x in ("FAILED", "CANCELLED", "ERROR")):
                            return {"error": f"Dune execution ended with state: {st}"}
                    elif r_res.status_code in (202, 425):  # accepted / too early
                        pass  # keep polling
                    else:
                        # Unexpected status; keep polling a bit longer then bail out
                        pass
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                return {"error": "Dune execution timed out waiting for results"}
            rows = (data.get("result") or {}).get("rows")
            if rows is None:
                rows = data.get("rows")
            if rows is not None:
                return {"rows": rows}
        # Fallback: legacy /sql endpoint if available
        # Fallback: legacy /sql endpoint if available
        r_sql = requests.post(DUNE_SQL_ENDPOINT, json={"sql": sql}, headers=headers, timeout=60)
        r_sql.raise_for_status()
        d = r_sql.json()
        rows = (d.get("result") or {}).get("rows")
        if rows is None:
            rows = d.get("rows")
        return {"rows": rows if rows is not None else []}
    except Exception as e:
        return {"error": str(e)}


def get_multi_swap_tx_count(contract_address: str, blockchain: str = "base") -> Optional[int]:
    addr = _sanitize_address(contract_address)
    if not addr:
        return None
    sql = f"""
    SELECT COUNT(*) AS multi_swap_tx_count
    FROM (
        SELECT tx_hash
        FROM dex.trades
        WHERE blockchain = '{blockchain}'
          AND (
                tx_from = {addr}
             OR tx_to   = {addr}
          )
        GROUP BY tx_hash
        HAVING COUNT(*) >= 2
    ) AS t
    """
    res = run_sql(sql)
    if "error" in res:
        return None
    rows = res.get("rows", [])
    if not rows:
        return 0
    if isinstance(rows[0], dict):
        val = rows[0].get("multi_swap_tx_count")
    else:
        val = rows[0][0] if rows[0] else 0
    try:
        return int(val)
    except Exception:
        return None


def get_liquidation_tx_count(contract_address: str, blockchain: str = "base") -> Optional[int]:
    addr = _sanitize_address(contract_address)
    if not addr:
        return None
    # Preferred canonical tables (chain-specific)
    if blockchain == "base":
        aave_table = "aave_v3_base.pool_evt_liquidationcall"
        morpho_table = "morpho_blue_base.morphoblue_evt_liquidate"
        sql = f"""
        WITH txs AS (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM {aave_table}
          WHERE liquidator = {addr}
          UNION
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM {morpho_table}
          WHERE caller = {addr}
        )
        SELECT COUNT(*) AS liquidation_tx_count
        FROM txs
        """
        res = run_sql(sql)
        if "error" not in res:
            rows = res.get("rows", [])
            if not rows:
                return 0
            if isinstance(rows[0], dict):
                val = rows[0].get("liquidation_tx_count")
            else:
                val = rows[0][0] if rows[0] else 0
            try:
                return int(val)
            except Exception:
                pass
    # Fallback candidates for other chains or if canonical query fails
    candidates = [
        f"""
        SELECT COUNT(*) AS liquidation_tx_count
        FROM (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM aave_v3_{blockchain}.LiquidationCall
          WHERE liquidator = '{addr}'
        ) t
        """,
        f"""
        SELECT COUNT(*) AS liquidation_tx_count
        FROM (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM aave_v3.{blockchain}.LiquidationCall
          WHERE liquidator = '{addr}'
        ) t
        """,
        f"""
        SELECT COUNT(*) AS liquidation_tx_count
        FROM (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM morpho_blue_{blockchain}.Liquidate
          WHERE liquidator = '{addr}' OR caller = '{addr}'
        ) t
        """,
        f"""
        SELECT COUNT(*) AS liquidation_tx_count
        FROM (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM morpho_{blockchain}.Liquidate
          WHERE liquidator = '{addr}' OR caller = '{addr}'
        ) t
        """,
        f"""
        SELECT COUNT(*) AS liquidation_tx_count
        FROM (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM morpho.{blockchain}.Liquidate
          WHERE liquidator = '{addr}' OR caller = '{addr}'
        ) t
        """,
        f"""
        SELECT COUNT(*) AS liquidation_tx_count
        FROM (
          SELECT DISTINCT evt_tx_hash AS tx_hash
          FROM compound_v3_{blockchain}.LiquidateBorrow
          WHERE liquidator = '{addr}'
        ) t
        """,
    ]
    for sql in candidates:
        res = run_sql(sql)
        if "error" in res:
            continue
        rows = res.get("rows", [])
        if not rows:
            return 0
        if isinstance(rows[0], dict):
            val = rows[0].get("liquidation_tx_count")
        else:
            val = rows[0][0] if rows[0] else 0
        try:
            return int(val)
        except Exception:
            continue
    return None
