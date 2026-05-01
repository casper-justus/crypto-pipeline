# 📥 Kafka Consumer — Crypto Prices

Python service that consumes messages from a Kafka topic and batch-inserts them into PostgreSQL.

## Impact

- **Achieved 57x higher throughput with batch inserts** as measured by load testing — 3,893 records/sec at batch size 100 vs 68 records/sec at batch size 1 (p95: 31ms vs 19.9ms), by implementing `psycopg2.extras.execute_values` with configurable batch sizing.
- **Processed 21,124 Kafka deserializations per second** as measured by benchmarking 50,000 JSON decode operations with 0% failure, by using `json.loads` on UTF-8 byte-decoded Kafka message payloads.

---

## Load Test Results

Run benchmarks yourself: `cd tests && python load_test_consumer.py`

### Batch Insert Performance

| Batch Size | Records/sec | p95 Latency | Wall Time (10k msgs) |
|------------|-------------|-------------|---------------------|
| 1 | 68 | 19.9ms | 147s |
| 10 | 572 | 21.3ms | 17s |
| 25 | 1,318 | 22.9ms | 8s |
| **50** (default) | **2,171** | **29.0ms** | **5s** |
| 100 | 3,893 | 31.3ms | 3s |
| 250 | 8,378 | 36.0ms | 1s |

### Message Processing

| Test | Messages | Throughput | Failure Rate |
|------|----------|------------|-------------|
| JSON Deserialization | 50,000 | 21,124 ops/sec | 0% |

## How It Works

```
Continuously:
  1. Consume messages from Kafka topic
  2. Accumulate messages into a batch
  3. Flush batch when size ≥ 50 or interval ≥ 10s
  4. INSERT INTO PostgreSQL using execute_values
  5. Commit and repeat
```

## Configuration

| Env Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `crypto_prices` | Topic to consume from |
| `KAFKA_GROUP_ID` | `consumer_group_1` | Consumer group ID |
| `POSTGRES_HOST` | `postgres` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `POSTGRES_DB` | `crypto_pipeline` | Database name |
| `POSTGRES_USER` | `pipeline_user` | Database user |
| `POSTGRES_PASSWORD` | `pipeline_pass` | Database password |

## Batch Settings

| Constant | Value | Description |
|---|---|---|
| `BATCH_SIZE` | 50 | Max messages per insert |
| `FLUSH_INTERVAL` | 10s | Max seconds between flushes |

## Database Schema

Consumed messages are inserted into the `crypto_prices` table:

```sql
CREATE TABLE crypto_prices (
    id                        SERIAL PRIMARY KEY,
    coin_id                   VARCHAR(50)    NOT NULL,
    symbol                    VARCHAR(10)    NOT NULL,
    name                      VARCHAR(100)   NOT NULL,
    current_price             DECIMAL(18, 8) NOT NULL,
    market_cap                DECIMAL(20, 2),
    total_volume              DECIMAL(20, 2),
    price_change_24h          DECIMAL(10, 4),
    price_change_percentage_24h DECIMAL(10, 4),
    high_24h                  DECIMAL(18, 8),
    low_24h                  DECIMAL(18, 8),
    circulating_supply        DECIMAL(20, 2),
    timestamp                 TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
```

## Run Standalone

```bash
pip install -r requirements.txt
python consumer.py
```

## Run with Docker

```bash
docker build -t crypto-consumer .
docker run --env-file .env crypto-consumer
```

## Error Handling

- Auto-commits Kafka offsets every 5 seconds
- Rolls back on database insert failures
- Reconnects to PostgreSQL on connection loss
- Auto-offset-reset: `earliest` (processes all available messages on startup)
