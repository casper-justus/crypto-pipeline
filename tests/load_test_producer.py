import concurrent.futures
import json
import statistics
import time
from dataclasses import dataclass, field

import requests


@dataclass
class LoadTestResult:
    name: str
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    latencies: list[float] = field(default_factory=list)
    throughput_per_sec: float = 0.0
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
        total_time = sum(self.latencies)
        if total_time > 0:
            self.throughput_per_sec = self.total_requests / total_time

    def summary(self) -> str:
        return (
            f"  Requests:      {self.successful}/{self.total_requests} "
            f"({self.failed} failed)\n"
            f"  Throughput:    {self.throughput_per_sec:.1f} req/sec\n"
            f"  Avg Latency:   {self.avg_latency * 1000:.0f} ms\n"
            f"  p50 Latency:   {self.p50_latency * 1000:.0f} ms\n"
            f"  p95 Latency:   {self.p95_latency * 1000:.0f} ms\n"
            f"  p99 Latency:   {self.p99_latency * 1000:.0f} ms"
        )


def fetch_single_price(coin_id: str, session: requests.Session) -> tuple:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": coin_id, "vs_currencies": "usd"}
    start = time.perf_counter()
    try:
        response = session.get(url, params=params, timeout=10)
        latency = time.perf_counter() - start
        response.raise_for_status()
        return (True, latency, coin_id)
    except Exception as e:
        latency = time.perf_counter() - start
        return (False, latency, str(e))


def load_test_coingecko_api(
    coin_ids: list[str],
    concurrent_users: int = 10,
    requests_per_user: int = 10,
) -> LoadTestResult:
    print(f"\n{'=' * 60}")
    print("Load Test: CoinGecko API Fetch")
    print(f"  Concurrent users: {concurrent_users}")
    print(f"  Requests per user: {requests_per_user}")
    print(f"  Total requests: {concurrent_users * requests_per_user}")
    print(f"{'=' * 60}")

    result = LoadTestResult(name="CoinGecko API")
    total_requests = concurrent_users * requests_per_user
    result.total_requests = total_requests

    session = requests.Session()

    def worker_task(_):
        user_results = []
        for coin_id in coin_ids[: len(coin_ids) // concurrent_users + 1]:
            if len(user_results) >= requests_per_user:
                break
            user_results.append(fetch_single_price(coin_id, session))
        return user_results

    start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrent_users
    ) as executor:
        futures = [executor.submit(worker_task, i) for i in range(concurrent_users)]
        for future in concurrent.futures.as_completed(futures):
            for success, latency, _detail in future.result():
                result.latencies.append(latency)
                if success:
                    result.successful += 1
                else:
                    result.failed += 1

    elapsed = time.perf_counter() - start
    result.calculate()
    print(f"  Total time: {elapsed:.2f}s")
    print(result.summary())
    return result


def load_test_message_serialization(
    num_messages: int = 10000,
) -> LoadTestResult:
    print(f"\n{'=' * 60}")
    print("Load Test: Message Serialization (JSON encode)")
    print(f"  Messages: {num_messages}")
    print(f"{'=' * 60}")

    result = LoadTestResult(name="Message Serialization")
    result.total_requests = num_messages

    sample_message = {
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

    for i in range(num_messages):
        msg = sample_message.copy()
        msg["coin_id"] = f"test_{i % 8}"
        start = time.perf_counter()
        try:
            json.dumps(msg).encode("utf-8")
            latency = time.perf_counter() - start
            result.latencies.append(latency)
            result.successful += 1
        except Exception:
            latency = time.perf_counter() - start
            result.latencies.append(latency)
            result.failed += 1

    result.calculate()
    print(f"  Total time: {sum(result.latencies):.4f}s")
    print(result.summary())
    return result


def load_test_producer_pipeline(
    coin_ids: list[str],
    concurrent_users: int = 5,
    rounds: int = 20,
) -> tuple:
    print(f"\n{'=' * 60}")
    print("Load Test: Full Producer Pipeline (API + Serialization)")
    print(f"  Concurrent users: {concurrent_users}")
    print(f"  Rounds per user: {rounds}")
    print(f"  Total cycles: {concurrent_users * rounds}")
    print(f"{'=' * 60}")

    api_result = LoadTestResult(name="Producer API")
    total_cycles = concurrent_users * rounds
    api_result.total_requests = total_cycles * len(coin_ids)

    session = requests.Session()

    def producer_worker(_):
        results = []
        for _ in range(rounds):
            for coin_id in coin_ids:
                success, latency, _ = fetch_single_price(coin_id, session)
                results.append((success, latency))
        return results

    start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrent_users
    ) as executor:
        futures = [executor.submit(producer_worker, i) for i in range(concurrent_users)]
        for future in concurrent.futures.as_completed(futures):
            for success, latency in future.result():
                api_result.latencies.append(latency)
                if success:
                    api_result.successful += 1
                else:
                    api_result.failed += 1

    elapsed = time.perf_counter() - start
    api_result.calculate()
    print(f"  Total time: {elapsed:.2f}s")
    print(api_result.summary())

    daily_messages = api_result.throughput_per_sec * 60 * 60 * 24
    print(f"\n  Projected daily throughput: {daily_messages:,.0f} messages/day")

    return api_result


def main():
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

    print("\n🔫 PRODUCER LOAD TEST SUITE")
    print("=" * 60)

    results = []

    results.append(load_test_message_serialization(num_messages=10000))
    results.append(
        load_test_coingecko_api(coin_ids, concurrent_users=4, requests_per_user=10)
    )
    api_result = load_test_producer_pipeline(coin_ids, concurrent_users=4, rounds=10)
    results.append(api_result)

    print(f"\n{'=' * 60}")
    print("📊 PRODUCER LOAD TEST SUMMARY")
    print(f"{'=' * 60}")
    for r in results:
        print(f"\n  [{r.name}]")
        print(r.summary())

    print("\n  Key metrics for README:")
    print(
        f"  - Daily projected messages: {api_result.throughput_per_sec * 60 * 60 * 24:,.0f}"
    )
    print(
        f"  - Success rate: {api_result.successful / api_result.total_requests * 100:.1f}%"
    )
    print(f"  - p95 API latency: {api_result.p95_latency * 1000:.0f}ms")


if __name__ == "__main__":
    main()
