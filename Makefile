.PHONY: install lint format test test-load all clean

install:
	pip install -r requirements.txt
	pip install -r dev-requirements.txt

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

test:
	pytest tests/test_producer.py tests/test_consumer.py -v

test-load:
	python3 tests/load_test_runner.py

all: lint test

clean:
	rm -rf .pytest_cache/ __pycache__/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
