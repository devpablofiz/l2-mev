CREATE TABLE IF NOT EXISTS blocks (
    block_number BIGINT PRIMARY KEY,
    block_hash TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    tx_hash TEXT PRIMARY KEY,
    block_number BIGINT REFERENCES blocks(block_number),
    from_address TEXT NOT NULL,
    to_address TEXT,
    value NUMERIC,
    gas_limit BIGINT,
    gas_price BIGINT,
    max_fee_per_gas BIGINT,
    max_priority_fee_per_gas BIGINT,
    input_data TEXT,
    nonce BIGINT,
    transaction_index INTEGER,
    status INTEGER -- 1 for success, 0 for failure
);

CREATE TABLE IF NOT EXISTS transaction_logs (
    log_id SERIAL PRIMARY KEY,
    tx_hash TEXT REFERENCES transactions(tx_hash),
    address TEXT NOT NULL,
    topic0 TEXT,
    topic1 TEXT,
    topic2 TEXT,
    topic3 TEXT,
    data TEXT,
    log_index INTEGER,
    UNIQUE (tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_transactions_from_address ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address ON transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON transaction_logs(tx_hash);
CREATE INDEX IF NOT EXISTS idx_logs_address ON transaction_logs(address);
CREATE INDEX IF NOT EXISTS idx_logs_topic0 ON transaction_logs(topic0);

-- Optimized index for MEV tagging
CREATE INDEX IF NOT EXISTS idx_transactions_method_id ON transactions (SUBSTRING(input_data, 1, 10));

-- Tagging Tables for MEV Analysis
CREATE TABLE IF NOT EXISTS method_tags (
    method_id TEXT PRIMARY KEY, -- First 10 chars of input_data
    tag_name TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS address_tags (
    address TEXT PRIMARY KEY,
    tag_name TEXT NOT NULL,
    source_method_id TEXT REFERENCES method_tags(method_id),
    description TEXT
);
