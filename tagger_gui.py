import streamlit as st
import os
import requests
import json
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import warnings

import db_utils

# MUST BE FIRST
st.set_page_config(page_title="MEV Method Tagger", layout="wide")

# Load environment variables
load_dotenv()

# Silence pandas DBAPI2 warning
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")

# DB Connection Details
TENDERLY_RPC_URL = os.getenv("TENDERLY_RPC_URL")

def trace_transaction_tenderly(tx_hash):
    """Traces a transaction using Tenderly's trace_transaction RPC method."""
    if not TENDERLY_RPC_URL:
        return "Error: TENDERLY_RPC_URL not found in .env"
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tenderly_traceTransaction",
        "params": ["0x"+tx_hash]
    }
    
    try:
        response = requests.post(TENDERLY_RPC_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            return f"RPC Error: {data['error'].get('message', 'Unknown error')}"
        
        return data.get("result", {})
    except Exception as e:
        return f"Request Error: {str(e)}"

def get_db_connection():
    return db_utils.get_db_connection()

def get_sqlalchemy_engine():
    return db_utils.get_sqlalchemy_engine()

st.title("🛡️ MEV Method Tagger")
st.markdown("Analyze and tag the most used calldata methods to identify bots and spam.")

def fetch_next_sample(method_id, exclude_tx, exclude_to):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT tx_hash, input_data, to_address, block_number
        FROM transactions
        WHERE method_id = %s
        ORDER BY block_number DESC
        LIMIT 30
    """, (method_id,))
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    ex_tx = set(exclude_tx or [])
    ex_to = set((a or "").lower() for a in (exclude_to or []))
    for tx, inp, to_addr, _bn in rows:
        to_norm = (to_addr or "").lower()
        if tx in ex_tx:
            continue
        if to_norm and to_norm in ex_to:
            continue
        return tx, inp
    return None

# If we are here, data_loaded is True
# Default tags for quick selection
DEFAULT_TAGS = [
    "Uniswap V4 Swap",
    "Uniswap V3 Swap",
    "Uniswap V2 Swap",
    "ERC20 Transfer",
    "ERC20 Approval",
    "MEV Bot: Arbitrage",
    "MEV Bot: Sandwich",
    "MEV Bot: Liquidation",
    "Contract Deployment",
    "Multicall",
    "Spam / Junk",
    "Eth Transfer",
    "Incentive Farming / Liquidity Manager",
    "Account Abstraction",
    "Liquidity Tick Maintainance?",
    "Gelato Keepalive"
]

@st.cache_data(ttl=3600)
def get_all_method_frequencies():
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
    SELECT 
        method_id,
        usage_count
    FROM method_frequencies
    ORDER BY usage_count DESC
    LIMIT 1000;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@st.cache_data(ttl=600)
def fetch_untagged_methods(limit=10):
    conn = db_utils.get_db_connection()
    cur = conn.cursor()
    try:
        db_utils.ensure_method_frequencies_view()
    except Exception:
        pass
    
    # Use the pre-calculated sample_tx_hash from method_frequencies view
    # This avoids hitting the massive transactions table during load
    query = """
    SELECT 
        mf.method_id,
        mf.usage_count,
        mf.sample_tx_hash
    FROM method_frequencies mf
    LEFT JOIN method_tags mt ON mf.method_id = mt.method_id
    WHERE mt.method_id IS NULL
    ORDER BY mf.usage_count DESC
    LIMIT %s;
    """
    cur.execute(query, (limit,))
    rows = cur.fetchall()
    
    results = []
    for mid, count, sample_tx in rows:
        results.append({
            "method_id": mid,
            "usage_count": count,
            "sample_tx_hash": sample_tx,
        })
            
    cur.close()
    conn.close()
    return results

@st.cache_data(ttl=600)
def fetch_tagged_methods_stats():
    """Fetch stats for tagged methods without sample transactions for speed."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Get all tagged methods with their frequencies in ONE query
    query = """
    SELECT 
        mt.method_id, 
        mt.tag_name, 
        mt.description,
        COALESCE(mf.usage_count, 0) as usage_count,
        mf.sample_tx_hash
    FROM method_tags mt
    LEFT JOIN method_frequencies mf ON mt.method_id = mf.method_id
    ORDER BY mf.usage_count DESC NULLS LAST;
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    results = []
    for mid, tag_name, description, count, sample_tx in rows:
        results.append({
            "method_id": mid,
            "tag_name": tag_name,
            "description": description,
            "usage_count": count,
            "sample_tx_hash": sample_tx,
            "samples": [] # No samples by default for speed
        })
         
    cur.close()
    conn.close()
    return results

def save_method_tag(method_id, tag_name, description):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO method_tags (method_id, tag_name, description) VALUES (%s, %s, %s) ON CONFLICT (method_id) DO UPDATE SET tag_name = EXCLUDED.tag_name, description = EXCLUDED.description",
        (method_id, tag_name, description)
    )
    conn.commit()
    cur.close()
    conn.close()

def run_auto_tagging():
    conn = get_db_connection()
    cur = conn.cursor()
    # Step 3 logic from example_queries.sql
    query = """
    WITH top_senders AS (
        SELECT from_address
        FROM transactions
        GROUP BY from_address
        ORDER BY COUNT(*) DESC
        LIMIT 200
    ),
    sender_recent_txs AS (
        SELECT 
            from_address,
            method_id,
            ROW_NUMBER() OVER (PARTITION BY from_address ORDER BY block_number DESC) as tx_rank
        FROM transactions
        WHERE from_address IN (SELECT from_address FROM top_senders)
    ),
    potential_tags AS (
        SELECT DISTINCT
            srt.from_address,
            mt.tag_name,
            mt.method_id,
            'Auto-tagged based on method usage in top 20 txs' as description
        FROM sender_recent_txs srt
        JOIN method_tags mt ON srt.method_id = mt.method_id
        WHERE srt.tx_rank <= 20
    )
    INSERT INTO address_tags (address, tag_name, source_method_id, description)
    SELECT from_address, tag_name, method_id, description FROM potential_tags
    ON CONFLICT (address) DO NOTHING;
    """
    cur.execute(query)
    conn.commit()
    count = cur.rowcount
    cur.close()
    conn.close()
    return count

def init_db():
    """Ensure tagging tables exist. NO ALTERING OF CORE SCHEMA."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS method_tags (
        method_id TEXT PRIMARY KEY,
        tag_name TEXT NOT NULL,
        description TEXT
    );
    CREATE TABLE IF NOT EXISTS address_tags (
        address TEXT PRIMARY KEY,
        tag_name TEXT NOT NULL,
        source_method_id TEXT REFERENCES method_tags(method_id),
        description TEXT
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

# Initialize database tables (DEFERRED)
# init_db()  <-- Moved inside the Load Data button logic

# 0. Load Data Guard
if 'data_loaded' not in st.session_state:
    st.info("👋 Welcome! Click the button below to load the dashboard data.")
    if st.button("🚀 Load Dashboard Data", use_container_width=True):
        init_db()
        db_utils.ensure_method_frequencies_view()
        st.session_state.data_loaded = True
        st.rerun()
    st.stop() # Absolutely stop here until they click

@st.cache_data(ttl=600)
def fetch_mev_exposure():
    """Fast part: Calculate exposure using pre-calculated frequencies."""
    # 1. Get pre-calculated frequencies
    all_methods = get_all_method_frequencies()
    
    # 2. Get current tags
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT method_id, tag_name FROM method_tags")
    tags = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    # 3. Aggregate
    activity_counts = {}
    total_tx = 0
    
    for mid, count in all_methods:
        tag = tags.get(mid, 'Untagged / Unknown')
        activity_counts[tag] = activity_counts.get(tag, 0) + count
        total_tx += count
        
    if total_tx == 0:
        return pd.DataFrame(columns=['activity_type', 'tx_count', 'percent'])
        
    data = []
    for tag, count in activity_counts.items():
        data.append({
            'activity_type': tag,
            'tx_count': count,
            'percent': round((count / total_tx) * 100, 2)
        })
        
    df = pd.DataFrame(data).sort_values('tx_count', ascending=False)
    return df

@st.cache_data(ttl=60)
def fetch_stats():
    engine = get_sqlalchemy_engine()
    with engine.connect() as conn:
        df_methods = pd.read_sql("SELECT tag_name, COUNT(*) as count FROM method_tags GROUP BY tag_name", conn)
        df_addresses = pd.read_sql("SELECT tag_name, COUNT(*) as count FROM address_tags GROUP BY tag_name", conn)
    return df_methods, df_addresses

# UI Layout
st.sidebar.title("Data Controls")
if st.sidebar.button("🔄 Refresh Data", type="primary"):
    get_all_method_frequencies.clear()
    fetch_untagged_methods.clear()
    fetch_tagged_methods_stats.clear()
    fetch_mev_exposure.clear()
    fetch_stats.clear()
    st.rerun()

col1, col2 = st.columns([2, 1])

with col1:
    st.header("1. Untagged Methods")
    
    # Use a status container for better feedback
    with st.status("Fetching database records...", expanded=True) as status_box:
        status_box.write("1/5: Calculating global method frequencies...")
        all_freqs = get_all_method_frequencies()
        
        status_box.write("2/5: Identifying untagged methods & fetching samples...")
        untagged = fetch_untagged_methods()
        
        status_box.write("3/5: Loading tagged method details...")
        tagged_data = fetch_tagged_methods_stats()
        
        status_box.write("4/5: Calculating MEV exposure metrics...")
        df_exposure = fetch_mev_exposure()

        status_box.write("5/5: Finalizing global statistics...")
        df_methods, df_addresses = fetch_stats()
        
        status_box.update(label="✅ Dashboard loaded!", state="complete", expanded=False)
    
    if not untagged:
        st.success("No untagged methods found!")
    else:
        for item in untagged:
            method_id = item['method_id']
            count = item['usage_count']
            sample_tx = item['sample_tx_hash']
            
            with st.expander(f"Method: {method_id} ({count} txs)"):
                state_key = f"samples_{method_id}"
                if state_key not in st.session_state:
                    # Initialize with the sample from the materialized view
                    st.session_state[state_key] = [(sample_tx, "")] if sample_tx else []
                
                current_samples = st.session_state[state_key]
                if current_samples:
                    links = [f"[{tx}](https://basescan.org/tx/0x{tx})" for tx, _ in current_samples]
                    st.write("Sample TXs: " + ", ".join(links))

                    tx_options = [tx for tx, _ in current_samples]
                    selected_tx = st.selectbox("Select a sample to inspect", tx_options, key=f"sel_{method_id}")
                    
                    # Fetch input data only when needed
                    selected_input = ""
                    for tx, idata in current_samples:
                        if tx == selected_tx:
                            if not idata:
                                # Fetch input_data for the first time
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("SELECT input_data FROM transactions WHERE tx_hash = %s", (tx,))
                                row = cur.fetchone()
                                if row:
                                    idata = row[0]
                                    # Update session state
                                    for i, (t, _) in enumerate(st.session_state[state_key]):
                                        if t == tx:
                                            st.session_state[state_key][i] = (tx, idata)
                                cur.close()
                                conn.close()
                            selected_input = idata
                            break

                    trace_col1, _ = st.columns([1, 4])
                    with trace_col1:
                        if st.button("🔍 Trace selected", key=f"trace_{method_id}_{selected_tx}"):
                            with st.spinner("Fetching trace..."):
                                trace_result = trace_transaction_tenderly(selected_tx)
                                if isinstance(trace_result, str):
                                    st.error(trace_result)
                                else:
                                    st.session_state[f"trace_data_{method_id}_{selected_tx}"] = trace_result
                        if st.button("➕ Fetch another sample", key=f"more_{method_id}"):
                            with st.spinner("Fetching another sample..."):
                                ex_tx = [tx for tx, _ in current_samples]
                                ex_to = []
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("SELECT to_address FROM transactions WHERE tx_hash = ANY(%s)", (ex_tx,))
                                to_rows = cur.fetchall() or []
                                cur.close()
                                conn.close()
                                ex_to = [r[0] for r in to_rows if r and r[0]]
                                nxt = fetch_next_sample(method_id, ex_tx, ex_to)
                                if nxt:
                                    st.session_state[state_key].append(nxt)
                                    st.rerun()
                                else:
                                    st.info("No more unique-to samples found.")
                    
                    if f"trace_data_{method_id}_{selected_tx}" in st.session_state:
                        st.code(json.dumps(st.session_state[f"trace_data_{method_id}_{selected_tx}"], indent=2), language="json")

                    st.code(selected_input, language="text")
                else:
                    st.info("No sample transactions available for this method.")
                
                with st.form(key=f"form_{method_id}"):
                    tag_col1, tag_col2 = st.columns(2)
                    with tag_col1:
                        selected_tag = st.selectbox("Select Tag", ["Custom..."] + DEFAULT_TAGS, key=f"select_{method_id}")
                    with tag_col2:
                        custom_tag = st.text_input("Custom Tag Name", key=f"custom_{method_id}")
                    
                    description = st.text_area("Description", key=f"desc_{method_id}")
                    
                    final_tag = custom_tag if selected_tag == "Custom..." else selected_tag
                    
                    if st.form_submit_button("Save Tag"):
                        if not final_tag:
                            st.error("Please provide a tag name.")
                        else:
                            save_method_tag(method_id, final_tag, description)
                            # Only clear the specific caches needed
                            fetch_untagged_methods.clear()
                            fetch_tagged_methods_stats.clear()
                            fetch_mev_exposure.clear()
                            fetch_stats.clear()
                            st.success(f"Tagged {method_id} as {final_tag}!")
                            st.rerun()

    st.divider()
    st.header("2. Tagged Methods Explorer")
    
    if not tagged_data:
        st.info("No tagged methods yet.")
    else:
        for item in tagged_data:
            with st.expander(f"{item['tag_name']} ({item['method_id']}) - {item['usage_count']} txs"):
                st.write(f"**Description:** {item['description'] or 'No description'}")
                
                state_key_t = f"samples_tag_{item['method_id']}"
                if state_key_t not in st.session_state:
                    # Initialize with the sample from the stats if available
                    sample_tx = item.get('sample_tx_hash')
                    st.session_state[state_key_t] = [(sample_tx, "")] if sample_tx else []
                
                samples = st.session_state[state_key_t]
                if samples:
                    links = [f"[{tx}](https://basescan.org/tx/0x{tx})" for tx, _ in samples]
                    st.write("Sample TXs: " + ", ".join(links))

                    tx_options = [tx for tx, _ in samples]
                    selected_tx = st.selectbox("Select a sample to inspect", tx_options, key=f"tag_sel_{item['method_id']}")
                    
                    # Fetch input data only when needed
                    selected_input = ""
                    for tx, idata in samples:
                        if tx == selected_tx:
                            if not idata:
                                # Fetch input_data for the first time
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("SELECT input_data FROM transactions WHERE tx_hash = %s", (tx,))
                                row = cur.fetchone()
                                if row:
                                    idata = row[0]
                                    # Update session state
                                    for i, (t, _) in enumerate(st.session_state[state_key_t]):
                                        if t == tx:
                                            st.session_state[state_key_t][i] = (tx, idata)
                                cur.close()
                                conn.close()
                            selected_input = idata
                            break
                    
                    # Re-use trace functionality for the selected sample
                    trace_col1, _ = st.columns([1, 4])
                    with trace_col1:
                        if st.button("🔍 Trace selected", key=f"trace_tagged_{item['method_id']}_{selected_tx}"):
                            with st.spinner("Fetching trace..."):
                                trace_result = trace_transaction_tenderly(selected_tx)
                                if isinstance(trace_result, str):
                                    st.error(trace_result)
                                else:
                                    st.session_state[f"trace_tag_data_{item['method_id']}_{selected_tx}"] = trace_result
                        if st.button("➕ Fetch another sample", key=f"more_tag_{item['method_id']}"):
                            with st.spinner("Fetching another sample..."):
                                ex_tx = [tx for tx, _ in samples]
                                conn = get_db_connection()
                                cur = conn.cursor()
                                cur.execute("""
                                    SELECT 
                                        tx_hash,
                                        input_data,
                                        to_address,
                                        block_number
                                    FROM transactions
                                    WHERE method_id = %s
                                    ORDER BY block_number DESC
                                    LIMIT 30
                                """, (item['method_id'],))
                                rows = cur.fetchall() or []
                                cur.close()
                                conn.close()
                                ex_set = set(ex_tx)
                                to_seen = set()
                                for tx, inp, to_addr, _bn in rows:
                                    if tx in ex_set:
                                        continue
                                    to_norm = (to_addr or "").lower()
                                    if to_norm and to_norm in to_seen:
                                        continue
                                    st.session_state[state_key_t].append((tx, inp))
                                    st.rerun()
                                st.info("No more unique-to samples found.")
                    
                    if f"trace_tag_data_{item['method_id']}_{selected_tx}" in st.session_state:
                        st.code(json.dumps(st.session_state[f"trace_tag_data_{item['method_id']}_{selected_tx}"], indent=2), language="json")
                    st.code(selected_input, language="text")
                else:
                    st.info("No sample transactions available for this method.")
                
                # Option to re-tag or edit
                with st.form(key=f"edit_form_{item['method_id']}"):
                    tag_col1, tag_col2 = st.columns(2)
                    with tag_col1:
                        selected_tag = st.selectbox("Update Tag", ["Custom..."] + DEFAULT_TAGS, 
                                                  index=DEFAULT_TAGS.index(item['tag_name'])+1 if item['tag_name'] in DEFAULT_TAGS else 0,
                                                  key=f"edit_select_{item['method_id']}")
                    with tag_col2:
                        custom_tag = st.text_input("Custom Tag Name", 
                                                 value=item['tag_name'] if item['tag_name'] not in DEFAULT_TAGS else "",
                                                 key=f"edit_custom_{item['method_id']}")
                    
                    description = st.text_area("Update Description", value=item['description'] or "", key=f"edit_desc_{item['method_id']}")
                    
                    final_tag = custom_tag if selected_tag == "Custom..." else selected_tag
                    
                    if st.form_submit_button("Update Tag"):
                        save_method_tag(item['method_id'], final_tag, description)
                        fetch_untagged_methods.clear()
                        fetch_tagged_methods_stats.clear()
                        fetch_mev_exposure.clear()
                        fetch_stats.clear()
                        st.success(f"Updated {item['method_id']}!")
                        st.rerun()

with col2:
    st.header("3. Actions")
    if st.button("🚀 Run Auto-Tagging for Senders", use_container_width=True):
        with st.spinner("Analyzing top senders..."):
            count = run_auto_tagging()
            # Clear everything including frequencies for a fresh start
            st.cache_data.clear()
            st.success(f"Tagged {count} new addresses based on their recent activity!")

    if st.button("🧮 Refresh Method Frequencies View", use_container_width=True):
        with st.spinner("Refreshing materialized view..."):
            db_utils.refresh_method_frequencies(concurrently=True)
            get_all_method_frequencies.clear()
            fetch_untagged_methods.clear()
            fetch_tagged_methods_stats.clear()
            fetch_mev_exposure.clear()
            st.success("Materialized view refreshed.")
            st.rerun()

    if st.button("🔄 Refresh Global Statistics", use_container_width=True):
        get_all_method_frequencies.clear()
        fetch_mev_exposure.clear()
        st.success("Global frequencies refreshed!")
        st.rerun()

    st.divider()
    
    st.header("4. Statistics")
    st.subheader("Method Tags")
    st.dataframe(df_methods, hide_index=True, use_container_width=True)
    
    st.subheader("Address Tags")
    st.dataframe(df_addresses, hide_index=True, use_container_width=True)

    st.divider()
    st.header("5. MEV Exposure")
    
    if not df_exposure.empty:
        # Pie Chart
        fig = px.pie(df_exposure, values='tx_count', names='activity_type', 
                    title='Transaction Distribution by Tag',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_traces(textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

        st.write("Distribution of transaction types:")
        st.dataframe(df_exposure, hide_index=True, use_container_width=True)
        
        # Simple progress bar for "How much of the network have I tagged?"
        tagged_total = df_exposure[df_exposure['activity_type'] != 'Untagged / Unknown']['percent'].sum()
        st.metric("Total Tagged Volume", f"{tagged_total:.2f}%")
        st.progress(min(tagged_total / 100.0, 1.0))
    else:
        st.info("Tag some methods to see exposure analysis.")
