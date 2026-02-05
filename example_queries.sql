-- Top 10 most active addresses (potential bots)
-- High transaction counts from a single address often indicate bot activity.
SELECT 
    from_address, 
    COUNT(*) as tx_count,
    COUNT(CASE WHEN status = 0 THEN 1 END) as failed_count,
    ROUND(COUNT(CASE WHEN status = 0 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC * 100, 2) as failure_rate_pct
FROM transactions
GROUP BY from_address
ORDER BY tx_count DESC
LIMIT 50;

-- IMPOSTOR DETECTION: Find tagged methods that might be misidentified or shared
-- This looks for methods that are called on many different contracts or have varying lengths,
-- which suggests the selector might have multiple different underlying functions (collisions).
SELECT 
    mt.method_id,
    mt.tag_name,
    COUNT(DISTINCT t.to_address) as unique_contracts,
    COUNT(DISTINCT LENGTH(t.input_data)) as unique_input_lengths,
    COUNT(*) as total_calls,
    MIN(LENGTH(t.input_data)) as min_len,
    MAX(LENGTH(t.input_data)) as max_len,
    -- A high diversity score suggests the tag might be too broad or a collision exists
    CASE 
        WHEN COUNT(DISTINCT t.to_address) > 10 THEN 'High Contract Diversity (Potential Generic Method)'
        WHEN COUNT(DISTINCT LENGTH(t.input_data)) > 5 THEN 'High Length Variance (Check for Collisions)'
        ELSE 'Consistent'
    END as status_check
FROM transactions t
JOIN method_tags mt ON SUBSTRING(t.input_data, 1, 10) = mt.method_id
GROUP BY mt.method_id, mt.tag_name
ORDER BY unique_contracts DESC;

-- Find specific "impostor" contracts for a specific tagged method
-- Replace '0xa9059cbb' with the method ID you want to investigate. 
-- This helps you see which contracts are using the same selector.
SELECT 
    to_address,
    COUNT(*) as call_count,
    MIN(input_data) as sample_data
FROM transactions
WHERE SUBSTRING(input_data, 1, 10) = '0xa9059cbb' 
GROUP BY to_address
ORDER BY call_count DESC
LIMIT 20;

-- Analyze most used calldata by the top transactioners
-- Goal: Identify the most frequent payloads from high-volume senders for efficient tagging.
WITH top_senders AS (
    SELECT from_address, COUNT(*) as sender_tx_count
    FROM transactions
    GROUP BY from_address
    ORDER BY sender_tx_count DESC
    LIMIT 50
),
calldata_stats AS (
    SELECT 
        t.input_data,
        SUBSTRING(t.input_data, 1, 10) as method_id,
        COUNT(*) as usage_count,
        COUNT(DISTINCT t.from_address) as unique_top_senders
    FROM transactions t
    JOIN top_senders ts ON t.from_address = ts.from_address
    WHERE t.input_data <> '0x'
    GROUP BY t.input_data, method_id
)
SELECT 
    method_id,
    input_data,
    usage_count,
    unique_top_senders,
    ROUND(usage_count::NUMERIC / (SELECT SUM(sender_tx_count) FROM top_senders)::NUMERIC * 100, 2) as percent_of_top_sender_txs
FROM calldata_stats
ORDER BY usage_count DESC
LIMIT 100;

-- TAGGING WORKFLOW: Step 1 - Identify top untagged methods
-- Use this to find which method IDs you should research and tag next.
SELECT 
    SUBSTRING(input_data, 1, 10) as method_id,
    COUNT(*) as usage_count,
    COUNT(DISTINCT from_address) as unique_senders,
    MIN(tx_hash) as sample_tx_hash
FROM transactions
LEFT JOIN method_tags mt ON SUBSTRING(input_data, 1, 10) = mt.method_id
WHERE mt.method_id IS NULL AND input_data <> '0x'
GROUP BY SUBSTRING(input_data, 1, 10)
ORDER BY usage_count DESC
LIMIT 20;

-- TAGGING WORKFLOW: Step 2 - Example of how to tag a method
-- INSERT INTO method_tags (method_id, tag_name, description) 
-- VALUES ('0x3593564c', 'Uniswap V3 Swap', 'Execution of a swap on Uniswap V3');

-- TAGGING WORKFLOW: Step 3 - Identify and tag senders based on their top 20 transactions
-- This query finds top senders who have used a tagged method at least once in their last 20 txs.
WITH top_senders AS (
    SELECT from_address
    FROM transactions
    GROUP BY from_address
    ORDER BY COUNT(*) DESC
    LIMIT 100
),
sender_recent_txs AS (
    SELECT 
        from_address,
        input_data,
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
SELECT * FROM potential_tags
-- To actually tag them, you would use:
-- INSERT INTO address_tags (address, tag_name, source_method_id, description)
-- SELECT from_address, tag_name, method_id, description FROM potential_tags
-- ON CONFLICT (address) DO NOTHING;
;

-- Senders with multiple transactions in the same block
-- Arbitrage bots often send multiple transactions in a single block to compete for the same opportunity.
SELECT 
    block_number, 
    from_address, 
    COUNT(*) as tx_in_block
FROM transactions
GROUP BY block_number, from_address
HAVING COUNT(*) > 1
ORDER BY tx_in_block DESC, block_number DESC
LIMIT 20;

-- Average Gas Price and Priority Fee for active bots
-- MEV bots often use higher priority fees to get their transactions included earlier.
SELECT 
    from_address,
    AVG(gas_price) / 1e9 as avg_gas_price_gwei,
    AVG(max_priority_fee_per_gas) / 1e9 as avg_priority_fee_gwei,
    COUNT(*) as tx_count
FROM transactions
WHERE max_priority_fee_per_gas IS NOT NULL
GROUP BY from_address
HAVING COUNT(*) > 10
ORDER BY avg_priority_fee_gwei DESC
LIMIT 10;

-- Detect "Spam" patterns: High volume of failed transactions
-- Bots that spam the network often have high failure rates as they lose races.
SELECT 
    from_address, 
    COUNT(*) as total_txs,
    COUNT(CASE WHEN status = 0 THEN 1 END) as failures
FROM transactions
GROUP BY from_address
HAVING COUNT(*) > 50 AND (COUNT(CASE WHEN status = 0 THEN 1 END)::FLOAT / COUNT(*)) > 0.5
ORDER BY failures DESC;

-- Find transactions with ERC20 Transfer events
-- The first topic (topic0) for an ERC20 Transfer event is always:
-- 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
SELECT 
    t.tx_hash, 
    t.block_number,
    t.from_address,
    t.to_address
FROM transactions t
JOIN transaction_logs l ON t.tx_hash = l.tx_hash
WHERE l.topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
LIMIT 20;

-- Extract Transfer details (Token address, From, To, Value)
-- Note: value is in hex in the 'data' field and needs conversion.
-- From and To are in topic1 and topic2 respectively.
SELECT 
    tx_hash,
    address as token_contract,
    topic1 as transfer_from,
    topic2 as transfer_to,
    data as raw_value
FROM transaction_logs
WHERE topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
LIMIT 20;

-- Combined Bot Score "many calls, few methods, few targets"
WITH stats AS (
  SELECT
    from_address,
    COUNT(*) AS tx_count,
    COUNT(DISTINCT to_address) AS targets,
    COUNT(DISTINCT SUBSTRING(input_data,1,10)) AS methods
  FROM transactions
  WHERE input_data <> '0x' AND value = 0
  GROUP BY from_address
)
SELECT *
FROM stats
WHERE
  tx_count > 100
  AND methods <= 3
ORDER BY tx_count DESC;

-- Contract addresses being spammed: (pools, routers, MEV helper)
SELECT
  to_address,
  COUNT(*) AS inbound_calls
FROM transactions
WHERE input_data <> '0x'
GROUP BY to_address
HAVING COUNT(*) > 500
ORDER BY inbound_calls DESC;

-- Low Gas Limit Variance
SELECT
  from_address,
  COUNT(*) AS txs,
  STDDEV(gas_limit) AS gas_stddev
FROM transactions
WHERE input_data <> '0x'
GROUP BY from_address
HAVING COUNT(*) > 50
ORDER BY gas_stddev ASC;

-- Repeated CallData
SELECT
  from_address,
  SUBSTRING(input_data, 1, 10) AS method_id,
  COUNT(*) AS calls
FROM transactions
WHERE input_data <> '0x'
GROUP BY from_address, method_id
HAVING COUNT(*) > 50
ORDER BY calls DESC;

-- Percent of total transactions sent by each address
-- This helps identify the most dominant entities on the network.
SELECT 
    from_address, 
    COUNT(*) as tx_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM transactions)::NUMERIC * 100, 4) as percent_of_total
FROM transactions
GROUP BY from_address
ORDER BY tx_count DESC
LIMIT 50;