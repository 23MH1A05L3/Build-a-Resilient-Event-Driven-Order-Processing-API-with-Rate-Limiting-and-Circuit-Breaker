import os
import json
import pika

EXCHANGE_NAME = "order_events"


def publish_order_created(order_event: dict) -> None:
    """
    Publishes an OrderCreated event to the 'order_events' topic exchange.
    Raises on any connection/publish failure so the caller's circuit
    breaker can record it.
    """
    credentials = pika.PlainCredentials(
        os.environ.get("RABBITMQ_USER", "guest"),
        os.environ.get("RABBITMQ_PASSWORD", "guest"),
    )
    parameters = pika.ConnectionParameters(
        host=os.environ.get("RABBITMQ_HOST", "localhost"),
        credentials=credentials,
        connection_attempts=1,
        socket_timeout=3,
    )

    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key="order.created",
            body=json.dumps(order_event),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                content_type="application/json",
                message_id=order_event["event_id"],
            ),
        )
    finally:
        connection.close()
