"""Unit tests for the consumer module."""

from unittest.mock import MagicMock, patch

import pytest

from src.consumer.main import (
    create_consumer,
    deserialize_message,
    insert_batch,
    message_to_row,
)

SAMPLE_MESSAGE = {
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


class TestDeserializeMessage:
    """Tests for deserialize_message."""

    def test_happy_path_returns_dict(self) -> None:
        raw = b'{"coin_id": "bitcoin", "price": 50000}'
        result = deserialize_message(raw)
        assert result == {"coin_id": "bitcoin", "price": 50000}
        assert isinstance(result, dict)

    def test_unicode_content(self) -> None:
        raw = b'{"name": "Coin\\u00e9"}'
        result = deserialize_message(raw)
        assert result["name"] == "Coin\u00e9"

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(Exception, match="Expecting value"):
            deserialize_message(b"not valid json")


class TestMessageToRow:
    """Tests for message_to_row transformation."""

    def test_happy_path_all_fields(self) -> None:
        row = message_to_row(SAMPLE_MESSAGE)
        assert row[0] == "bitcoin"
        assert row[1] == "btc"
        assert row[2] == "Bitcoin"
        assert row[3] == 67543.21
        assert row[10] == 19600000

    def test_missing_optional_fields_defaults_to_none(self) -> None:
        minimal = {"coin_id": "x", "symbol": "x", "name": "X", "current_price": 1.0}
        row = message_to_row(minimal)
        assert row[4] is None
        assert row[5] is None
        assert row[10] is None

    def test_tuple_length(self) -> None:
        row = message_to_row(SAMPLE_MESSAGE)
        assert len(row) == 11


class TestInsertBatch:
    """Tests for insert_batch with mocked psycopg2."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @patch("src.consumer.main.execute_values")
    def test_happy_path_single_record(
        self, mock_execute: MagicMock, mock_conn: MagicMock
    ) -> None:
        count = insert_batch([SAMPLE_MESSAGE], mock_conn)
        assert count == 1
        mock_execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("src.consumer.main.execute_values")
    def test_happy_path_multiple_records(
        self, mock_execute: MagicMock, mock_conn: MagicMock
    ) -> None:
        messages = [
            {**SAMPLE_MESSAGE, "coin_id": "bitcoin"},
            {**SAMPLE_MESSAGE, "coin_id": "ethereum"},
            {**SAMPLE_MESSAGE, "coin_id": "solana"},
        ]
        count = insert_batch(messages, mock_conn)
        assert count == 3
        mock_execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_empty_batch_returns_zero(self, mock_conn: MagicMock) -> None:
        count = insert_batch([], mock_conn)
        assert count == 0
        mock_conn.commit.assert_not_called()

    @patch("src.consumer.main.execute_values")
    def test_calls_execute_values_with_cursor(
        self, mock_execute: MagicMock, mock_conn: MagicMock
    ) -> None:
        insert_batch([SAMPLE_MESSAGE], mock_conn)
        cursor_arg = mock_execute.call_args[0][0]
        assert cursor_arg == mock_conn.cursor.return_value

    @patch("src.consumer.main.execute_values")
    def test_rollback_on_failure(
        self, mock_execute: MagicMock, mock_conn: MagicMock
    ) -> None:
        mock_execute.side_effect = Exception("DB error")
        with pytest.raises(Exception, match="DB error"):
            insert_batch([SAMPLE_MESSAGE], mock_conn)
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()


class TestCreateConsumer:
    """Tests for create_consumer factory."""

    @patch("src.consumer.main.KafkaConsumer")
    def test_configures_bootstrap_servers(self, mock_consumer: MagicMock) -> None:
        create_consumer(
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            group_id="test_group",
        )
        mock_consumer.assert_called_once()
        call_kwargs = mock_consumer.call_args.kwargs
        assert call_kwargs["bootstrap_servers"] == "localhost:9092"
        assert call_kwargs["auto_offset_reset"] == "earliest"
        assert call_kwargs["enable_auto_commit"] is True
