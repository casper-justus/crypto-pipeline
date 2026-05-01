import json
import os
import sqlite3
import statistics
import tempfile
import time
from dataclasses import dataclass, field

import requests


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
            f"  Throughput:    {self.throughput_per_sec:.2f} ops/sec\n"
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
"""


def e2e_cycle(coin_id: str, session: requests.Session, db_conn) -> dict:
    pipeline_start = time.perf_counter()

    api_start = time.perf_counter()
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": coin_id, "vs_currencies": "usd"}
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        api_data = response.json()
        price = api_data[coin_id]["usd"]
    except Exception as e:
        return {
            "success": False,
            "total_latency": time.perf_counter() - pipeline_start,
            "error": str(e),
            "coin_id": coin_id,
        }

    api_latency = time.perf_counter() - api_start

    serialize_start = time.perf_counter()
    message = {
        "coin_id": coin_id,
        "symbol": coin_id[:3],
        "name": coin_id.title(),
        "current_price": price,
        "market_cap": None,
        "total_volume": None,
        "price_change_24h": None,
        "price_change_percentage_24h": None,
        "high_24h": None,
        "low_24h": None,
        "circulating_supply": None,
    }
    raw = json.dumps(message).encode("utf-8")
    serialize_latency = time.perf_counter() - serialize_start

    deserialize_start = time.perf_counter()
    parsed = json.loads(raw.decode("utf-8"))
    deserialize_latency = time.perf_counter() - deserialize_start

    db_start = time.perf_counter()
    db_conn.execute(
        "INSERT INTO crypto_prices (coin_id, symbol, name, current_price) VALUES (?,?,?,?)",
        (parsed["coin_id"], parsed["symbol"], parsed["name"], parsed["current_price"]),
    )
    db_conn.commit()
    db_latency = time.perf_counter() - db_start

    total_latency = time.perf_counter() - pipeline_start

    return {
        "success": True,
        "total_latency": total_latency,
        "api_latency": api_latency,
        "serialize_latency": serialize_latency,
        "deserialize_latency": deserialize_latency,
        "db_latency": db_latency,
        "coin_id": coin_id,
    }


def load_test_sequential_pipeline(
    coin_ids: list[str],
    rounds: int = 5,
    delay_between_rounds: float = 2.0,
) -> dict:
    print(f"\n{'=' * 60}")
    print("Load Test: Sequential Pipeline (simulating 15s intervals)")
    print(f"  Coins: {len(coin_ids)}")
    print(f"  Rounds: {rounds}")
    print(f"  Delay between rounds: {delay_between_rounds}s")
    print(f"  Total cycles: {len(coin_ids) * rounds}")
    print(f"{'=' * 60}")

    result = LoadTestResult(name="Sequential Pipeline")
    total_cycles = len(coin_ids) * rounds
    result.total_ops = total_cycles

    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    session = requests.Session()

    stage_latencies = {"API": [], "Serialize": [], "Deserialize": [], "DB": []}

    wall_start = time.perf_counter()

    for round_idx in range(rounds):
        for coin_id in coin_ids:
            cycle = e2e_cycle(coin_id, session, conn)
            if cycle["success"]:
                result.latencies.append(cycle["total_latency"])
                result.successful += 1
                stage_latencies["API"].append(cycle["api_latency"])
                stage_latencies["Serialize"].append(cycle["serialize_latency"])
                stage_latencies["Deserialize"].append(cycle["deserialize_latency"])
                stage_latencies["DB"].append(cycle["db_latency"])
            else:
                result.failed += 1
                result.latencies.append(cycle["total_latency"])

        if round_idx < rounds - 1:
            time.sleep(delay_between_rounds)

    result.wall_time = time.perf_counter() - wall_start
    result.records_per_sec = result.successful / result.wall_time
    result.calculate()

    conn.close()
    os.unlink(db_path)

    print("\n  Stage breakdown:")
    for stage_name, latencies in stage_latencies.items():
        if latencies:
            avg = statistics.mean(latencies) * 1000
            p95 = sorted(latencies)[int(len(latencies) * 0.95)] * 1000
            print(f"    {stage_name:15s} avg={avg:.0f}ms  p95={p95:.0f}ms")

    print("\n  Overall:")
    print(result.summary())

    daily_cycles = result.throughput_per_sec * 60 * 60 * 24
    print(f"\n  Projected daily throughput: {daily_cycles:,.0f} cycles/day")

    return result


def load_test_kafka_overhead(iterations: int = 10000) -> LoadTestResult:
    print(f"\n{'=' * 60}")
    print("Load Test: Kafka Serialize/Deserialize Overhead")
    print(f"  Iterations: {iterations}")
    print(f"{'=' * 60}")

    result = LoadTestResult(name="Kafka Overhead")
    result.total_ops = iterations

    message = {
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
        "circulating_supply": 19600000,
    }

    wall_start = time.perf_counter()
    for _ in range(iterations):
        start = time.perf_counter()
        raw = json.dumps(message).encode("utf-8")
        _ = json.loads(raw.decode("utf-8"))
        latency = time.perf_counter() - start
        result.latencies.append(latency)
        result.successful += 1

    result.wall_time = time.perf_counter() - wall_start
    result.throughput_per_sec = result.successful / result.wall_time
    result.calculate()

    print(result.summary())
    return result


def main():
    coin_ids = ["bitcoin", "ethereum", "solana", "cardano"]

    print("\n🔫 END-TO-END PIPELINE LOAD TEST")
    print("=" * 60)

    kafka_result = load_test_kafka_overhead(iterations=10000)
    pipeline_result = load_test_sequential_pipeline(
        coin_ids=coin_ids,
        rounds=5,
        delay_between_rounds=2.0,
    )

    print(f"\n{'=' * 60}")
    print("📊 E2E SUMMARY FOR README")
    print(f"{'=' * 60}")
    print(
        f"  Kafka serialize+deserialize: {kafka_result.avg_latency * 1_000_000:.0f}μs avg"
    )
    print(f"  End-to-end p95 latency: {pipeline_result.p95_latency * 1000:.0f}ms")
    print(f"  Throughput: {pipeline_result.records_per_sec:.1f} cycles/sec")


if __name__ == "__main__":
    main()
