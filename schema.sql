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
    method_id TEXT, -- First 10 chars of input_data for faster querying
    nonce BIGINT,
    transaction_index INTEGER
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions(block_number);
CREATE INDEX IF NOT EXISTS idx_transactions_from_address ON transactions(from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address ON transactions(to_address);
CREATE INDEX IF NOT EXISTS idx_transactions_method_id ON transactions (method_id);

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
