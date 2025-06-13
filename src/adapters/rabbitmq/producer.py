import asyncio
import aio_pika
from typing import Any, Optional
from ...mq_abstraction_layer import AbstractProducer
from ...unified_message_model import Message # Assuming Message class has 'delay' and 'headers' attributes

class RabbitMQProducer(AbstractProducer):
    def __init__(self, amqp_url: str):
        """
        Initializes the RabbitMQProducer with the specified AMQP URL.
        
        Args:
            amqp_url: The AMQP connection URL for the RabbitMQ server.
        """
        self.amqp_url = amqp_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.Channel | None = None

    async def connect(self, **kwargs: Any) -> None:
        """
        Establishes an asynchronous robust connection and channel to RabbitMQ.
        
        Additional connection parameters can be provided via keyword arguments. Raises an exception if the connection or channel cannot be established.
        """
        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url, **kwargs)
            self.channel = await self.connection.channel()
            print("RabbitMQ Producer: Connected and channel established.")
        except Exception as e:
            print(f"RabbitMQ Producer: Error connecting: {e}")
            raise

    async def disconnect(self) -> None:
        """
        Closes the RabbitMQ channel and connection if they are open.
        
        Resets the internal channel and connection attributes to None after closing.
        """
        if self.channel:
            await self.channel.close()
            print("RabbitMQ Producer: Channel closed.")
        if self.connection:
            await self.connection.close()
            print("RabbitMQ Producer: Connection closed.")
        self.channel = None
        self.connection = None

    async def publish_message(
        self,
        message: Message,
        topic: str, # topic can be used as a routing key if routing_key is None
        exchange_name: str = "default_exchange",
        routing_key: Optional[str] = None,
        exchange_type: str = "direct", # Allow specifying exchange type, e.g., 'direct', 'fanout', 'topic', 'x-delayed-message'
        **kwargs: Any
    ) -> None:
        """
        Publishes a message to a RabbitMQ exchange with a specified routing key and exchange type.
        
        If the routing key is not provided, the topic is used as the routing key.
        The exchange is declared idempotently and can be of various types (direct, fanout, topic, or custom like x-delayed-message).
        The message is published with persistent delivery mode and includes metadata such as message ID, timestamp, and headers.
        If message.delay is set and positive, 'x-delay' header is added for delayed message plugins.
        
        Raises:
            ConnectionError: If the producer is not connected to RabbitMQ.
            Exception: If publishing the message fails.
        """
        if not self.channel or not self.connection or self.connection.is_closed:
            raise ConnectionError("RabbitMQ Producer is not connected.")

        # Default routing key to topic if not provided
        effective_routing_key = routing_key if routing_key is not None else topic

        try:
            # Ensure the exchange exists (idempotent)
            exchange = await self.channel.declare_exchange(
                name=exchange_name,
                type=exchange_type, # Use the provided exchange type
                durable=True,
                **kwargs.get("exchange_declare_kwargs", {})
            )

            # Handle message delay
            current_headers = message.headers if message.headers is not None else {}
            if hasattr(message, 'delay') and message.delay is not None and message.delay > 0:
                current_headers['x-delay'] = message.delay

            body_bytes = message.body if isinstance(message.body, bytes) else message.body.encode('utf-8')

            properties = aio_pika.BasicProperties(
                message_id=message.id,
                timestamp=message.timestamp,
                headers=current_headers, # Use potentially modified headers
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
