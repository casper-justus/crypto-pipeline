#!/usr/bin/env python3
"""
Load test runner for the crypto price pipeline.
Runs all load test suites and prints a unified report.

Usage:
    python load_test_runner.py [--quick]

Options:
    --quick    Run with fewer iterations for faster results
"""

import subprocess
import sys
import time
from datetime import datetime


def run_test(script: str, description: str) -> tuple:
    print(f"\n{'=' * 60}")
    print(f"Running: {description}")
    print(f"Script: {script}")
    print(f"{'=' * 60}")
    start = time.time()
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,
        text=True,
    )
    elapsed = time.time() - start
    return result.returncode, elapsed


def main():
    quick = "--quick" in sys.argv

    print("\n" + "=" * 60)
    print("CRYPTO PIPELINE — LOAD TEST REPORT")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'quick' if quick else 'full'}")
    print("=" * 60)

    tests = [
        ("tests/load_test_producer.py", "Producer Load Tests"),
        ("tests/load_test_consumer.py", "Consumer Load Tests"),
        ("tests/load_test_e2e.py", "End-to-End Pipeline Load Tests"),
    ]

    results = []
    for script, description in tests:
        rc, elapsed = run_test(script, description)
        results.append((description, rc, elapsed))
        status = "PASS" if rc == 0 else "FAIL"
        print(f"\n  [{status}] {description} — {elapsed:.0f}s")

    print(f"\n{'=' * 60}")
    print("FINAL SUMMARY")
    print(f"{'=' * 60}")
    for desc, rc, elapsed in results:
        status = "PASS" if rc == 0 else "FAIL"
        print(f"  [{status}] {desc} ({elapsed:.0f}s)")

    print("\nRun this script against a live pipeline for production metrics:")
    print("  docker compose up -d --build")
    print("  python load_test_runner.py")


if __name__ == "__main__":
    main()
