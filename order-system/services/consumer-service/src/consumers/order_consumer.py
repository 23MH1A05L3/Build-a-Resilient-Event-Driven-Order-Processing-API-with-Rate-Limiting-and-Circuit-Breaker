import os
import json
import time
import logging

import pika
from pika.exceptions import AMQPConnectionError

from database.db import get_connection
from database.models import (
    is_event_processed,
    mark_event_processed,
    create_order_in_db,
    deduct_inventory,
    update_order_status,
)

logger = logging.getLogger("consumer-service.order_consumer")

EXCHANGE_NAME = "order_events"
QUEUE_NAME = "order_processing_queue"
ROUTING_KEY = "order.created"

DLX_NAME = "order_events_dlx"
DLQ_NAME = "order_processing_dlq"

MAX_DELIVERY_ATTEMPTS = 3


def _get_connection_params():
    credentials = pika.PlainCredentials(
        os.environ.get("RABBITMQ_USER", "guest"),
        os.environ.get("RABBITMQ_PASSWORD", "guest"),
    )
    return pika.ConnectionParameters(
        host=os.environ.get("RABBITMQ_HOST", "localhost"),
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )


def _declare_topology(channel):
    # Main exchange (matches the publisher declaration)
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    # Dead letter exchange + queue for messages that repeatedly fail processing
    channel.exchange_declare(exchange=DLX_NAME, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DLQ_NAME, durable=True)
    channel.queue_bind(exchange=DLX_NAME, queue=DLQ_NAME)

    # Main processing queue, dead-letters to DLX when a message is nacked/rejected
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={"x-dead-letter-exchange": DLX_NAME},
    )
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)


def process_order_event(event_data: dict) -> None:
    """
    Parses an OrderCreated event, creates the order record, deducts
    inventory, and updates the order's final status. Idempotent: safe to
    call more than once for the same event_id.
    """
    event_id = event_data["event_id"]
    order_id = event_data["order_id"]

    conn = get_connection()
    try:
        if is_event_processed(conn, event_id):
            logger.info("Event %s already processed, skipping (idempotency).", event_id)
            return
    finally:
        conn.close()

    # Step 1: persist the order in 'pending' state (idempotent insert)
    create_order_in_db(event_data)

    # Step 2: attempt to deduct inventory
    success = deduct_inventory(event_data["product_id"], event_data["quantity"])

    if success:
        update_order_status(order_id, "completed")
        logger.info("Order %s completed successfully.", order_id)
    else:
        update_order_status(order_id, "failed", error_message="Insufficient stock")
        logger.warning("Order %s failed: insufficient stock.", order_id)

    # Step 3: record the event as processed (idempotency guard)
    conn = get_connection()
    try:
        mark_event_processed(conn, event_id, order_id)
        conn.commit()
    finally:
        conn.close()


def _on_message(channel, method, properties, body):
    try:
        event_data = json.loads(body)
        process_order_event(event_data)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        logger.error("Error processing message: %s", exc, exc_info=True)

        # Determine delivery attempt count via header (x-death set by broker on redelivery)
        headers = properties.headers or {}
        death_count = 0
        if "x-death" in headers:
            for death in headers["x-death"]:
                death_count = max(death_count, death.get("count", 0))

        if death_count + 1 >= MAX_DELIVERY_ATTEMPTS:
            logger.error("Max delivery attempts reached, sending to DLQ.")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            # Requeue for another attempt
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def start_consuming_orders():
    """Main loop: connects to the message queue and consumes messages,
    reconnecting automatically if the connection drops."""
    while True:
        try:
            connection = pika.BlockingConnection(_get_connection_params())
            channel = connection.channel()
            _declare_topology(channel)
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=_on_message)

            logger.info("Consumer started. Waiting for messages on '%s'...", QUEUE_NAME)
            channel.start_consuming()
        except AMQPConnectionError as exc:
            logger.error("Connection to RabbitMQ failed: %s. Retrying in 5s...", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Consumer shutting down.")
            break
        except Exception as exc:
            logger.error("Unexpected consumer error: %s. Retrying in 5s...", exc, exc_info=True)
            time.sleep(5)
