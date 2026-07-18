import logging
from database.db import get_connection

logger = logging.getLogger("consumer-service.models")


def is_event_processed(conn, event_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM processed_events WHERE event_id = %s", (event_id,))
        return cur.fetchone() is not None


def mark_event_processed(conn, event_id: str, order_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT IGNORE INTO processed_events (event_id, order_id) VALUES (%s, %s)",
            (event_id, order_id),
        )


def order_exists(conn, order_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM orders WHERE id = %s", (order_id,))
        return cur.fetchone() is not None


def create_order_in_db(order_data: dict) -> str:
    """Inserts a new order row with status 'pending'. Idempotent: if the
    order already exists (e.g. re-delivered message), does nothing."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT IGNORE INTO orders (id, product_id, quantity, customer_id, status) "
                "VALUES (%s, %s, %s, %s, 'pending')",
                (
                    order_data["order_id"],
                    order_data["product_id"],
                    order_data["quantity"],
                    order_data["customer_id"],
                ),
            )
        conn.commit()
        return order_data["order_id"]
    finally:
        conn.close()


def deduct_inventory(product_id: str, quantity: int) -> bool:
    """Atomically deducts stock if enough is available. Returns True on
    success, False if insufficient stock."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET stock_quantity = stock_quantity - %s "
                "WHERE id = %s AND stock_quantity >= %s",
                (quantity, product_id, quantity),
            )
            success = cur.rowcount == 1
        conn.commit()
        return success
    finally:
        conn.close()


def update_order_status(order_id: str, status: str, error_message: str = None) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = %s, error_message = %s WHERE id = %s",
                (status, error_message, order_id),
            )
        conn.commit()
    finally:
        conn.close()
