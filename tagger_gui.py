import streamlit as st
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import warnings

# Load environment variables
load_dotenv()

# Silence pandas DBAPI2 warning
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# DB Connection Details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "l2_mev")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_sqlalchemy_engine():
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

st.set_page_config(page_title="MEV Method Tagger", layout="wide")

st.title("🛡️ MEV Method Tagger")
st.markdown("Analyze and tag the most used calldata methods to identify bots and spam.")

# Default tags for quick selection
DEFAULT_TAGS = [
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
    """Heavy lifting: Count all method usages across the entire table.
    Cached for 1 hour because transaction counts don't change while tagging.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
    SELECT 
        SUBSTRING(input_data, 1, 10) as method_id,
        COUNT(*) as usage_count
    FROM transactions
    WHERE input_data <> '0x'
    GROUP BY SUBSTRING(input_data, 1, 10)
    ORDER BY usage_count DESC;
    """
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@st.cache_data(ttl=600)
def fetch_untagged_methods():
    """Fast part: Filter the pre-calculated frequencies against current tags."""
    # 1. Get pre-calculated frequencies (usually from cache)
    all_methods = get_all_method_frequencies()
    
    # 2. Get currently tagged methods
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT method_id FROM method_tags")
    tagged_ids = {row[0] for row in cur.fetchall()}
    
    # 3. Find top 10 untagged
    untagged_top = []
    for mid, count in all_methods:
        if mid not in tagged_ids:
            untagged_top.append((mid, count))
        if len(untagged_top) >= 10:
            break
            
    if not untagged_top:
        cur.close()
        conn.close()
        return []

    # 4. Get samples for only those 10
    results = []
    for mid, count in untagged_top:
        cur.execute("""
            SELECT 
                COUNT(DISTINCT from_address) as unique_senders,
                MIN(tx_hash) as sample_tx_hash,
                MIN(input_data) as sample_data
            FROM transactions 
            WHERE SUBSTRING(input_data, 1, 10) = %s
        """, (mid,))
        stats = cur.fetchone()
        results.append((mid, count, stats[0], stats[1], stats[2]))
        
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
            SUBSTRING(input_data, 1, 10) as method_id,
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
    """Ensure tagging tables exist."""
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
    CREATE INDEX IF NOT EXISTS idx_transactions_method_id ON transactions (SUBSTRING(input_data, 1, 10));
    """)
    conn.commit()
    cur.close()
    conn.close()

# Initialize database tables
init_db()

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
    df_methods = pd.read_sql("SELECT tag_name, COUNT(*) as count FROM method_tags GROUP BY tag_name", engine)
    df_addresses = pd.read_sql("SELECT tag_name, COUNT(*) as count FROM address_tags GROUP BY tag_name", engine)
    return df_methods, df_addresses

# UI Layout
col1, col2 = st.columns([2, 1])

with col1:
    st.header("1. Untagged Methods")
    untagged = fetch_untagged_methods()
    
    if not untagged:
        st.success("No untagged methods found!")
    else:
        for method_id, count, senders, tx_hash, sample in untagged:
            with st.expander(f"Method: {method_id} ({count} txs, {senders} senders)"):
                st.write(f"**Sample TX:** `{tx_hash}`")
                st.code(sample, language="text")
                
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
                            fetch_mev_exposure.clear()
                            fetch_stats.clear()
                            st.success(f"Tagged {method_id} as {final_tag}!")
                            st.rerun()

with col2:
    st.header("2. Actions")
    if st.button("🚀 Run Auto-Tagging for Senders", width='stretch'):
        with st.spinner("Analyzing top senders..."):
            count = run_auto_tagging()
            # Clear everything including frequencies for a fresh start
            st.cache_data.clear()
            st.success(f"Tagged {count} new addresses based on their recent activity!")

    if st.button("🔄 Refresh Global Statistics", width='stretch'):
        get_all_method_frequencies.clear()
        fetch_mev_exposure.clear()
        st.success("Global frequencies refreshed!")
        st.rerun()

    st.divider()
    
    st.header("3. Statistics")
    df_methods, df_addresses = fetch_stats()
    
    st.subheader("Method Tags")
    st.dataframe(df_methods, hide_index=True, width='stretch')
    
    st.subheader("Address Tags")
    st.dataframe(df_addresses, hide_index=True, width='stretch')

    st.divider()
    st.header("4. MEV Exposure")
    
    df_exposure = fetch_mev_exposure()
    
    st.write("Distribution of transaction types (non-value transfers):")
    st.dataframe(df_exposure, hide_index=True, width='stretch')
    
    # Simple progress bar for "How much of the network have I tagged?"
    tagged_total = df_exposure[df_exposure['activity_type'] != 'Untagged / Unknown']['percent'].sum()
    st.metric("Total Tagged Volume", f"{tagged_total:.2f}%")
    st.progress(min(tagged_total / 100.0, 1.0))
