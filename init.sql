CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    current_price DECIMAL(18, 8) NOT NULL,
    market_cap DECIMAL(20, 2),
    total_volume DECIMAL(20, 2),
    price_change_24h DECIMAL(10, 4),
    price_change_percentage_24h DECIMAL(10, 4),
    high_24h DECIMAL(18, 8),
    low_24h DECIMAL(18, 8),
    circulating_supply DECIMAL(20, 2),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_crypto_prices_coin_id ON crypto_prices(coin_id);
CREATE INDEX idx_crypto_prices_timestamp ON crypto_prices(timestamp);
CREATE INDEX idx_crypto_prices_coin_timestamp ON crypto_prices(coin_id, timestamp DESC);
