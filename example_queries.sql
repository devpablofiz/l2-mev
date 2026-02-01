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
LIMIT 10;

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