import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from consumers.order_consumer import process_order_event


SAMPLE_EVENT = {
    "event_id": "evt-1",
    "event_type": "OrderCreated",
    "order_id": "order-1",
    "product_id": "prod-101",
    "quantity": 2,
    "customer_id": "cust-1",
    "timestamp": "2026-07-18T00:00:00+00:00",
}


@patch("consumers.order_consumer.mark_event_processed")
@patch("consumers.order_consumer.update_order_status")
@patch("consumers.order_consumer.deduct_inventory", return_value=True)
@patch("consumers.order_consumer.create_order_in_db")
@patch("consumers.order_consumer.is_event_processed", return_value=False)
@patch("consumers.order_consumer.get_connection")
def test_process_order_event_success(
    mock_get_conn, mock_is_processed, mock_create, mock_deduct, mock_update_status, mock_mark
):
    mock_get_conn.return_value = MagicMock()
    process_order_event(SAMPLE_EVENT)

    mock_create.assert_called_once_with(SAMPLE_EVENT)
    mock_deduct.assert_called_once_with("prod-101", 2)
    mock_update_status.assert_called_once_with("order-1", "completed")
    mock_mark.assert_called_once()


@patch("consumers.order_consumer.mark_event_processed")
@patch("consumers.order_consumer.update_order_status")
@patch("consumers.order_consumer.deduct_inventory", return_value=False)
@patch("consumers.order_consumer.create_order_in_db")
@patch("consumers.order_consumer.is_event_processed", return_value=False)
@patch("consumers.order_consumer.get_connection")
def test_process_order_event_insufficient_stock(
    mock_get_conn, mock_is_processed, mock_create, mock_deduct, mock_update_status, mock_mark
):
    mock_get_conn.return_value = MagicMock()
    process_order_event(SAMPLE_EVENT)

    mock_update_status.assert_called_once_with("order-1", "failed", error_message="Insufficient stock")


@patch("consumers.order_consumer.mark_event_processed")
@patch("consumers.order_consumer.update_order_status")
@patch("consumers.order_consumer.deduct_inventory")
@patch("consumers.order_consumer.create_order_in_db")
@patch("consumers.order_consumer.is_event_processed", return_value=True)
@patch("consumers.order_consumer.get_connection")
def test_process_order_event_idempotent_skip(
    mock_get_conn, mock_is_processed, mock_create, mock_deduct, mock_update_status, mock_mark
):
    mock_get_conn.return_value = MagicMock()
    process_order_event(SAMPLE_EVENT)

    # Already processed -> none of the downstream side effects should run
    mock_create.assert_not_called()
    mock_deduct.assert_not_called()
    mock_update_status.assert_not_called()
