import asyncio
import aio_pika
from typing import Any
from ...mq_abstraction_layer import AbstractProducer
from ...unified_message_model import Message

class RabbitMQProducer(AbstractProducer):
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.Channel | None = None

    async def connect(self, **kwargs: Any) -> None:
        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url, **kwargs)
            self.channel = await self.connection.channel()
            print("RabbitMQ Producer: Connected and channel established.")
        except Exception as e:
            print(f"RabbitMQ Producer: Error connecting: {e}")
            raise

    async def disconnect(self) -> None:
        if self.channel:
            await self.channel.close()
            print("RabbitMQ Producer: Channel closed.")
        if self.connection:
            await self.connection.close()
            print("RabbitMQ Producer: Connection closed.")
        self.channel = None
        self.connection = None

    async def publish_message(self, message: Message, topic: str, exchange_name: str = "default_exchange", routing_key: str | None = None, **kwargs: Any) -> None:
        if not self.channel or not self.connection or self.connection.is_closed:
            raise ConnectionError("RabbitMQ Producer is not connected.")

        # Default routing key to topic if not provided
        effective_routing_key = routing_key if routing_key is not None else topic

        try:
            # Ensure the exchange exists (idempotent)
            exchange = await self.channel.declare_exchange(
                name=exchange_name,
                type=aio_pika.ExchangeType.DIRECT, # Or other types as needed
                durable=True,
                **kwargs.get("exchange_declare_kwargs", {})
            )

            body_bytes = message.body if isinstance(message.body, bytes) else message.body.encode('utf-8')

            properties = aio_pika.BasicProperties(
                message_id=message.id,
                timestamp=message.timestamp,
                headers=message.headers,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT # Make messages persistent
            )

            await exchange.publish(
                aio_pika.Message(
                    body=body_bytes,
                    properties=properties
                ),
                routing_key=effective_routing_key,
                **kwargs.get("publish_kwargs", {})
            )
            print(f"RabbitMQ Producer: Message '{message.id}' published to exchange '{exchange_name}' with routing key '{effective_routing_key}'.")
        except Exception as e:
            print(f"RabbitMQ Producer: Error publishing message: {e}")
            raise
