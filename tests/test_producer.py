"""Unit tests for the producer module."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.producer.main import (
    create_producer,
    fetch_prices,
    normalize_coin_data,
    serialize_message,
)

SAMPLE_COIN_API = {
    "id": "bitcoin",
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

EXPECTED_NORMALIZED = {
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


class TestNormalizeCoinData:
    """Tests for normalize_coin_data transformation."""

    def test_happy_path_all_fields(self) -> None:
        result = normalize_coin_data(SAMPLE_COIN_API)
        assert result == EXPECTED_NORMALIZED

    def test_missing_optional_fields(self) -> None:
        minimal = {"id": "test", "symbol": "tst", "name": "Test", "current_price": 1.0}
        result = normalize_coin_data(minimal)
        assert result["coin_id"] == "test"
        assert result["market_cap"] is None
        assert result["total_volume"] is None
        assert result["price_change_24h"] is None

    def test_preserves_values(self) -> None:
        result = normalize_coin_data(SAMPLE_COIN_API)
        assert result["current_price"] == 67543.21
        assert result["symbol"] == "btc"
        assert result["name"] == "Bitcoin"


class TestSerializeMessage:
    """Tests for serialize_message."""

    def test_happy_path_returns_bytes(self) -> None:
        message = {"coin_id": "bitcoin", "price": 50000}
        result = serialize_message(message)
        assert isinstance(result, bytes)
        assert json.loads(result) == message

    def test_utf8_encoding(self) -> None:
        message = {"name": "Coin\u00e9"}
        result = serialize_message(message)
        assert result.decode("utf-8")


class TestFetchPrices:
    """Tests for fetch_prices with mocked HTTP."""

    @patch("src.producer.main.requests.get")
    def test_happy_path(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(
            json=MagicMock(return_value=[SAMPLE_COIN_API]),
            raise_for_status=MagicMock(),
        )
        result = fetch_prices(
            api_url="https://fake-api.com",
            crypto_ids="bitcoin",
        )
        assert len(result) == 1
        assert result[0]["id"] == "bitcoin"
        mock_get.assert_called_once()

    @patch("src.producer.main.requests.get")
    def test_accepts_custom_session(self, mock_get: MagicMock) -> None:
        session = MagicMock()
        session.get.return_value = MagicMock(
            json=MagicMock(return_value=[SAMPLE_COIN_API]),
            raise_for_status=MagicMock(),
        )
        result = fetch_prices(
            api_url="https://fake-api.com",
            crypto_ids="bitcoin",
            session=session,
        )
        assert len(result) == 1
        session.get.assert_called_once()
        mock_get.assert_not_called()

    @patch("src.producer.main.requests.get")
    def test_raises_on_http_error(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock()
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError(
            "403 Forbidden"
        )
        with pytest.raises(requests.HTTPError):
            fetch_prices(api_url="https://fake-api.com", crypto_ids="bitcoin")


class TestCreateProducer:
    """Tests for create_producer factory."""

    @patch("src.producer.main.KafkaProducer")
    def test_configures_bootstrap_servers(self, mock_producer: MagicMock) -> None:
        create_producer(bootstrap_servers="localhost:9092")
        mock_producer.assert_called_once()
        call_kwargs = mock_producer.call_args.kwargs
        assert call_kwargs["bootstrap_servers"] == "localhost:9092"
        assert call_kwargs["acks"] == "all"
        assert call_kwargs["retries"] == 3
