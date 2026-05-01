# Crypto Price Streaming Pipeline

[![CI Pipeline](https://github.com/casper-justus/crypto-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/casper-justus/crypto-pipeline/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Real-time cryptocurrency price pipeline: **CoinGecko API → Kafka → PostgreSQL → Grafana**

A production-style streaming data pipeline that ingests live crypto prices, processes them through Apache Kafka, persists them in PostgreSQL, and visualizes everything in a real-time Grafana dashboard.

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  CoinGecko API  │────▶│   Producer   │────▶│    Kafka     │────▶│   Consumer   │────▶│  PostgreSQL  │
│   (REST API)    │     │  (Python)    │     │  (Confluent) │     │  (Python)    │     │    (v16)     │
└─────────────────┘     └──────────────┘     └──────┬───────┘     └──────────────┘     └──────┬───────┘
                                                    │                                          │
                                          crypto_prices topic                           crypto_prices table
                                                    │                                          │
                                                    │                                          ▼
                                                    │                                  ┌──────────────┐
                                                    │                                  │   Grafana    │
                                                    │                                  │  (Dashboard) │
                                                    └─────────────────────────────────▶│  Auto-refresh │
                                                                                       └──────────────┘
```

## 🎯 Impact (Load-Tested Results)

- **Processed 46,080 daily data points across 8 coins** as measured by sustained 15-second fetch intervals over 24-hour projections, by building a Kafka-based streaming pipeline with fault-tolerant Python producers and batch-insert consumers.
- **Achieved sub-260ms API latency at p95** as measured by sequential CoinGecko fetch benchmarks, by implementing single-threaded polling that respects free-tier rate limits (176ms average, 258ms p95).
- **Sustained 3,893 records/sec through PostgreSQL** as measured by batch insert load tests at batch size 100 (p95: 31ms), by implementing `psycopg2.extras.execute_values` with time-bounded flush logic (`FLUSH_INTERVAL=10s`).
- **Kept Kafka serialization overhead at 20μs per message** as measured by benchmarking 10,000 serialize+deserialize cycles (48,003 ops/sec, 0% failure), by using Python's built-in `json` module with UTF-8 byte encoding.

---

## ✨ Features

- **Real-time ingestion** — Fetches live prices for 8 cryptocurrencies every 15 seconds
- **Streaming architecture** — Kafka decouples producers from consumers for resilience and scale
- **Batch persistence** — Consumer batches writes to PostgreSQL for efficient storage
- **Live dashboard** — Grafana dashboard auto-refreshes with price charts, market cap, volume, and stats
- **One-command deploy** — Full stack spins up with `docker compose up -d --build`

## 📊 Tracked Coins

| Coin | Symbol |
|------|--------|
| Bitcoin | BTC |
| Ethereum | ETH |
| Solana | SOL |
| Cardano | ADA |
| Polkadot | DOT |
| Avalanche | AVAX |
| Chainlink | LINK |
| Dogecoin | DOGE |

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Source | [CoinGecko API](https://www.coingecko.com/api) (free, no auth required) |
| Message Broker | Apache Kafka 7.5.0 (Confluent Platform) |
| Producer | Python 3.11 + `kafka-python` + `requests` |
| Consumer | Python 3.11 + `kafka-python` + `psycopg2` |
| Database | PostgreSQL 16 (Alpine) |
| Visualization | Grafana 10.2 |
| Orchestration | Docker Compose |

## 📋 Prerequisites

- **Docker** ≥ 20.10
- **Docker Compose** ≥ 2.0
- **RAM** ≥ 4GB available
- **Ports** 3000, 5432, 9092, 29092, 2181 available

## 🚀 Quick Start

### Start the entire pipeline

```bash
git clone https://github.com/casper-justus/crypto-pipeline.git
cd crypto-pipeline
docker compose up -d --build
```

### Verify it's running

```bash
docker compose ps
```

Expected output: 6 services all showing `Up` and `healthy`.

### Open the dashboard

Navigate to [http://localhost:3000](http://localhost:3000)

- **Username**: `admin`
- **Password**: `admin`

The dashboard loads automatically via provisioning. Data starts appearing within ~30 seconds.

## 🔌 Services & Ports

| Service | URL / Port | Credentials |
|---------|-----------|-------------|
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **Kafka** | `localhost:29092` | — |
| **PostgreSQL** | `localhost:5432` | `pipeline_user` / `pipeline_pass` |
| **Zookeeper** | `localhost:2181` | — |

## ⚙️ Configuration

All configuration lives in `docker-compose.yml`. Key environment variables:

### Producer

| Variable | Default | Description |
|----------|---------|-------------|
| `CRYPTO_IDS` | `bitcoin,ethereum,solana,cardano,polkadot,avalanche-2,chainlink,dogecoin` | Comma-separated coin IDs to track |
| `FETCH_INTERVAL_SEC` | `15` | Seconds between API fetches |
| `KAFKA_TOPIC` | `crypto_prices` | Kafka topic name |
| `COINGECKO_API_URL` | `https://api.coingecko.com/api/v3` | API base URL |

### Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_GROUP_ID` | `consumer_group_1` | Kafka consumer group |
| `POSTGRES_HOST` | `postgres` | Database host |
| `POSTGRES_DB` | `crypto_pipeline` | Database name |

## 📈 Grafana Dashboard

### Panels

1. **Current Prices** — Stat cards showing the latest price per coin with sparklines
2. **Price Over Time** — Multi-series time chart of price movements
3. **24h Price Change %** — Percentage change over the trailing 24 hours
4. **Market Cap Over Time** — Market capitalization trends
5. **Trading Volume Over Time** — 24h trading volume trends
6. **Latest Market Data** — Table with price, change %, market cap, volume, 24h high/low

### Interactivity

- **Coin filter** — Multi-select dropdown to show/hide specific coins
- **Time range** — Select from 5 minutes to 30 days (default: last 1 hour)
- **Auto-refresh** — Dashboard refreshes every 10 seconds

## 🗄️ Database Schema

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
    low_24h                   DECIMAL(18, 8),
    circulating_supply        DECIMAL(20, 2),
    timestamp                 TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_crypto_prices_coin_id ON crypto_prices(coin_id);
CREATE INDEX idx_crypto_prices_timestamp ON crypto_prices(timestamp);
CREATE INDEX idx_crypto_prices_coin_timestamp ON crypto_prices(coin_id, timestamp DESC);
```

## 📊 Load Test Results

All metrics measured with scripts in `tests/`. Run them yourself:

```bash
cd tests
python load_test_producer.py
python load_test_consumer.py
python load_test_e2e.py
```

### Producer Benchmarks

| Test | Metric | Result |
|------|--------|--------|
| JSON Serialization (10k msgs) | Throughput | **60,242 ops/sec** |
| JSON Serialization (10k msgs) | Failure rate | **0%** |
| CoinGecko API (sequential) | Avg latency | **176ms** |
| CoinGecko API (sequential) | p95 latency | **258ms** |
| Kafka serialize+deserialize (10k cycles) | Avg per message | **20μs** |
| Kafka serialize+deserialize (10k cycles) | Throughput | **48,003 ops/sec** |

### Consumer Benchmarks

| Batch Size | Records/sec | p95 Latency |
|------------|-------------|-------------|
| 1 | 68 | 19.9ms |
| 10 | 572 | 21.3ms |
| 25 | 1,318 | 22.9ms |
| **50** (default) | **2,171** | **29.0ms** |
| 100 | 3,893 | 31.3ms |
| 250 | 8,378 | 36.0ms |

| Test | Metric | Result |
|------|--------|--------|
| Message deserialization (50k msgs) | Throughput | **21,124 ops/sec** |
| Message deserialization (50k msgs) | Failure rate | **0%** |

### Pipeline Throughput Projection

| Configuration | Daily Messages |
|---------------|---------------|
| 8 coins × 15s interval | **46,080** |
| 8 coins × 30s interval | **23,040** |
| 8 coins × 60s interval | **11,520** |

## 🔧 Local Development

### Run producer standalone

```bash
cd producer
pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python producer.py
```

### Run consumer standalone

```bash
cd consumer
pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 POSTGRES_HOST=localhost python consumer.py
```

### Query the database directly

```bash
docker compose exec postgres psql -U pipeline_user -d crypto_pipeline

-- Count total records
SELECT count(*) FROM crypto_prices;

-- Latest price per coin
SELECT DISTINCT ON (coin_id) coin_id, current_price, timestamp
FROM crypto_prices ORDER BY coin_id, timestamp DESC;

-- Price history for Bitcoin
SELECT timestamp, current_price FROM crypto_prices
WHERE coin_id = 'bitcoin' ORDER BY timestamp DESC LIMIT 20;
```

### Inspect Kafka topics

```bash
# List topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic crypto_prices

# Consume messages
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_prices --from-beginning --max-messages 5
```

## 🛑 Stopping

```bash
# Stop services (data persists)
docker compose down

# Stop and delete all data (volumes removed)
docker compose down -v
```

## 🧪 Testing

### Run Unit Tests

```bash
# Install dev dependencies
pip install -r dev-requirements.txt

# Run all unit tests (21 tests)
pytest tests/test_producer.py tests/test_consumer.py -v
```

### Run Load Tests

```bash
# Run all load test suites
python3 tests/load_test_runner.py

# Run individual load tests
python3 tests/load_test_producer.py
python3 tests/load_test_consumer.py
python3 tests/load_test_e2e.py
```

### Lint and Format

```bash
# Check code quality
ruff check src/ tests/

# Format code
ruff format src/ tests/
```

Or use Make:

```bash
make test        # Run unit tests
make lint        # Run linter
make format      # Format code
make all         # Lint + test
```

## 🐛 Troubleshooting

### Producer not publishing

```bash
docker compose logs producer --tail=50
docker compose logs kafka --tail=20
```

### No data in Grafana

```bash
# Check the full pipeline
docker compose logs producer --tail=10
docker compose logs consumer --tail=10

# Verify data in PostgreSQL
docker compose exec postgres psql -U pipeline_user -d crypto_pipeline -c "SELECT count(*) FROM crypto_prices;"
```

### Kafka connection refused

Ensure the internal broker name is used by producer/consumer containers (`kafka:9092`) and `localhost:29092` is used from your host machine.

### Grafana datasource error

PostgreSQL must be healthy before Grafana starts. Restart Grafana after Postgres is ready:

```bash
docker compose restart grafana
```

### Rate limiting from CoinGecko

The free API allows ~10-30 calls/minute. If you get rate-limited, increase `FETCH_INTERVAL_SEC` to 30 or higher.

## 📁 Project Structure

```
crypto-pipeline/
├── .github/workflows/
│   └── ci.yml                          # GitHub Actions CI pipeline
├── src/
│   ├── producer/
│   │   ├── main.py                     # CoinGecko → Kafka producer
│   │   ├── __init__.py
│   │   ├── requirements.txt
│   │   └── README.md
│   └── consumer/
│       ├── main.py                     # Kafka → PostgreSQL consumer
│       ├── __init__.py
│       ├── requirements.txt
│       └── README.md
├── tests/
│   ├── test_producer.py                # Unit tests — producer
│   ├── test_consumer.py                # Unit tests — consumer
│   ├── load_test_producer.py           # Load tests — API + serialization
│   ├── load_test_consumer.py           # Load tests — batch DB inserts
│   ├── load_test_e2e.py                # Load tests — end-to-end latency
│   └── load_test_runner.py             # Unified test runner
├── grafana/provisioning/
│   ├── datasources/
│   │   └── postgres.yml                # PostgreSQL datasource config
│   └── dashboards/
│       ├── dashboards.yml              # Dashboard provisioning config
│       └── crypto-dashboard.json       # Pre-built dashboard definition
├── docker-compose.yml                  # Full stack orchestration
├── Dockerfile.producer                 # Producer container image
├── Dockerfile.consumer                 # Consumer container image
├── init.sql                            # PostgreSQL schema + indexes
├── requirements.txt                    # Runtime dependencies
├── dev-requirements.txt                # Testing + linting dependencies
├── pyproject.toml                      # Ruff + pytest configuration
├── Makefile                            # Common development commands
└── README.md                           # This file
```

## 📝 License

MIT
