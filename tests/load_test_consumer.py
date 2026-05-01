import json
import os
import sqlite3
import statistics
import tempfile
import time
from dataclasses import dataclass, field


@dataclass
class LoadTestResult:
    name: str
    total_ops: int = 0
    successful: int = 0
    failed: int = 0
    latencies: list[float] = field(default_factory=list)
    wall_time: float = 0.0
    throughput_per_sec: float = 0.0
    records_per_sec: float = 0.0
    p50_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0
    avg_latency: float = 0.0

    def calculate(self):
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            self.avg_latency = statistics.mean(self.latencies)
            self.p50_latency = sorted_latencies[int(len(sorted_latencies) * 0.5)]
            self.p95_latency = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            self.p99_latency = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        if self.wall_time > 0:
            self.throughput_per_sec = self.successful / self.wall_time

    def summary(self) -> str:
        return (
            f"  Operations:    {self.successful}/{self.total_ops} "
            f"({self.failed} failed)\n"
            f"  Wall time:     {self.wall_time * 1000:.0f} ms\n"
            f"  Throughput:    {self.throughput_per_sec:.1f} batches/sec\n"
            f"  Records/sec:   {self.records_per_sec:,.0f}\n"
            f"  Avg Latency:   {self.avg_latency * 1000:.1f} ms\n"
            f"  p50 Latency:   {self.p50_latency * 1000:.1f} ms\n"
            f"  p95 Latency:   {self.p95_latency * 1000:.1f} ms\n"
            f"  p99 Latency:   {self.p99_latency * 1000:.1f} ms"
        )


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    current_price REAL NOT NULL,
    market_cap REAL,
    total_volume REAL,
    price_change_24h REAL,
    price_change_percentage_24h REAL,
    high_24h REAL,
    low_24h REAL,
    circulating_supply REAL,
    timestamp TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_coin_id ON crypto_prices(coin_id);
CREATE INDEX IF NOT EXISTS idx_timestamp ON crypto_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_coin_timestamp ON crypto_prices(coin_id, timestamp DESC);
"""


def get_sample_message(coin_id: str, index: int) -> tuple:
    base_prices = {
        "bitcoin": 67500,
        "ethereum": 3400,
        "solana": 145,
        "cardano": 0.45,
        "polkadot": 7.2,
        "avalanche-2": 35,
        "chainlink": 14,
        "dogecoin": 0.12,
    }
    price = base_prices.get(coin_id, 10)
    return (
        coin_id,
        coin_id[:3],
        coin_id.title(),
        price * (1 + index * 0.0001),
        price * 10_000_000,
        price * 1_000_000,
        price * 0.02,
        2.0,
        price * 1.03,
        price * 0.97,
        19_600_000,
    )


def load_test_batch_inserts(
    batch_sizes: list[int],
    total_messages: int = 10000,
) -> list[LoadTestResult]:
    print(f"\n{'=' * 60}")
    print("Load Test: Batch Database Inserts (SQLite simulation)")
    print(f"  Total messages: {total_messages}")
    print(f"  Batch sizes tested: {batch_sizes}")
    print(f"{'=' * 60}")

    all_results = []
    coin_ids = [
        "bitcoin",
        "ethereum",
        "solana",
        "cardano",
        "polkadot",
        "avalanche-2",
        "chainlink",
        "dogecoin",
    ]

    for batch_size in batch_sizes:
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        conn.executescript(SCHEMA_SQL)
        conn.commit()

        result = LoadTestResult(name=f"Batch Size {batch_size}")
        num_batches = total_messages // batch_size
        result.total_ops = num_batches
        batch_size_val = batch_size

        # Build batches
        batches = []
        for batch_idx in range(num_batches):
            batch = []
            for i in range(batch_size_val):
                msg = get_sample_message(
                    coin_ids[i % 8], batch_idx * batch_size_val + i
                )
                batch.append(msg)
            batches.append(batch)

        # Execute batches
        wall_start = time.perf_counter()
        for batch in batches:
            start = time.perf_counter()
            try:
                placeholders = ",".join(["(?,?,?,?,?,?,?,?,?,?,?)"] * len(batch))
                values = []
                for row in batch:
                    values.extend(row)
                conn.execute(
                    f"INSERT INTO crypto_prices (coin_id, symbol, name, current_price, market_cap, total_volume, price_change_24h, price_change_percentage_24h, high_24h, low_24h, circulating_supply) VALUES {placeholders}",
                    values,
                )
                conn.commit()
                latency = time.perf_counter() - start
                result.latencies.append(latency)
                result.successful += 1
            except Exception as e:
                latency = time.perf_counter() - start
                result.latencies.append(latency)
                result.failed += 1
                print(f"  ERROR: {e}")

        result.wall_time = time.perf_counter() - wall_start
        result.records_per_sec = result.successful * batch_size_val / result.wall_time
        result.calculate()

        conn.close()
        os.unlink(db_path)

        print(f"\n  [{result.name}]")
        print(result.summary())
        all_results.append(result)

    return all_results


def load_test_message_parsing(
    num_messages: int = 50000,
) -> LoadTestResult:
    print(f"\n{'=' * 60}")
    print("Load Test: Kafka Message Deserialization")
    print(f"  Messages: {num_messages}")
    print(f"{'=' * 60}")

    result = LoadTestResult(name="Message Deserialization")
    result.total_ops = num_messages

    coin_ids = [
        "bitcoin",
        "ethereum",
        "solana",
        "cardano",
        "polkadot",
        "avalanche-2",
        "chainlink",
        "dogecoin",
    ]

    wall_start = time.perf_counter()
    for i in range(num_messages):
        coin_id = coin_ids[i % 8]
        row = get_sample_message(coin_id, i)
        serialized = json.dumps(
            {
                "coin_id": row[0],
                "symbol": row[1],
                "name": row[2],
                "current_price": row[3],
                "market_cap": row[4],
                "total_volume": row[5],
                "price_change_24h": row[6],
                "price_change_percentage_24h": row[7],
                "high_24h": row[8],
                "low_24h": row[9],
                "circulating_supply": row[10],
            }
        )
        start = time.perf_counter()
        try:
            json.loads(serialized)
            latency = time.perf_counter() - start
            result.latencies.append(latency)
            result.successful += 1
        except Exception:
            latency = time.perf_counter() - start
            result.latencies.append(latency)
            result.failed += 1

    result.wall_time = time.perf_counter() - wall_start
    result.throughput_per_sec = result.successful / result.wall_time
    result.calculate()

    print(f"  Total time: {result.wall_time:.4f}s")
    print(result.summary())
    return result


def main():
    print("\n🔫 CONSUMER LOAD TEST SUITE")
    print("=" * 60)

    results = []
    results.append(load_test_message_parsing(num_messages=50000))
    batch_results = load_test_batch_inserts(
        batch_sizes=[1, 10, 25, 50, 100, 250],
        total_messages=10000,
    )
    results.extend(batch_results)

    print(f"\n{'=' * 60}")
    print("📊 CONSUMER LOAD TEST SUMMARY")
    print(f"{'=' * 60}")
    for r in results:
        print(f"\n  [{r.name}]")
        print(f"    Throughput: {r.records_per_sec:,.0f} records/sec")
        print(f"    p95 latency: {r.p95_latency * 1000:.1f}ms")

    best = max(batch_results, key=lambda x: x.records_per_sec)
    print(f"\n  Optimal batch size: {best.name.split()[-1]}")
    print(f"  Peak throughput: {best.records_per_sec:,.0f} records/sec")


if __name__ == "__main__":
    main()
