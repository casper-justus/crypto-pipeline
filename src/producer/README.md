# 📤 Kafka Producer — Crypto Prices

Python service that fetches live cryptocurrency prices from the CoinGecko API and publishes them to an Apache Kafka topic.

## Impact

- **Serialized 60,242 JSON messages per second** as measured by benchmarking 10,000 message encodings with 0% failure rate, by using Python's built-in `json` module with UTF-8 byte encoding.
- **Kept Kafka serialize+deserialize overhead at 20μs per message** as measured by 10,000 round-trip cycles (48,003 ops/sec), by using keyed `coin_id` serialization for partition efficiency.
- **Maintained 176ms average API latency (258ms p95)** as measured by sequential CoinGecko fetch benchmarks, by implementing single-threaded polling at 15-second intervals that respects free-tier rate limits.

---

## Load Test Results

Run benchmarks yourself: `cd tests && python load_test_producer.py`

| Test | Messages | Throughput | Failure Rate |
|------|----------|------------|-------------|
| JSON Serialization | 10,000 | 60,242 ops/sec | 0% |
| Kafka serialize+deserialize | 10,000 | 48,003 ops/sec | 0% |
| CoinGecko API (sequential) | — | avg 176ms / p95 258ms | varies by rate limit |

## How It Works

```
Every N seconds:
  1. GET /api/v3/coins/markets from CoinGecko
  2. Parse response into standardized JSON messages
  3. Publish each coin as a Kafka message (keyed by coin_id)
  4. Flush and wait for next interval
```

## Configuration

| Env Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `crypto_prices` | Topic to publish to |
| `COINGECKO_API_URL` | `https://api.coingecko.com/api/v3` | API base URL |
| `CRYPTO_IDS` | `bitcoin,ethereum,solana,...` | Comma-separated coin IDs |
| `FETCH_INTERVAL_SEC` | `15` | Seconds between API calls |

## Message Format

Each Kafka message is a JSON object with a `coin_id` key:

```json
{
  "coin_id": "bitcoin",
  "symbol": "btc",
  "name": "Bitcoin",
  "current_price": 67543.21,
  "market_cap": 1325000000000,
  "total_volume": 28500000000,
  "price_change_24h": 1234.56,
  "price_change_percentage_24h": 1.86,
  "high_24h": 68000.00,
  "low_24h": 66200.00,
  "circulating_supply": 19600000
}
```

## Run Standalone

```bash
pip install -r requirements.txt
python producer.py
```

## Run with Docker

```bash
docker build -t crypto-producer .
docker run --env-file .env crypto-producer
```

## Error Handling

- Retries API requests up to 3 times on failure
- Logs errors and continues on transient failures
- Gracefully handles rate limiting from CoinGecko
