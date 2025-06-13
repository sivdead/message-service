import asyncio
import aio_pika
from typing import Any, Callable
from ...mq_abstraction_layer import AbstractConsumer
from ...unified_message_model import Message
from datetime import datetime, timezone

class RabbitMQConsumer(AbstractConsumer):
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.Channel | None = None
        self.queue: aio_pika.Queue | None = None
        self._consuming_task: asyncio.Task | None = None
        self._callback: Callable[[Message], Any] | None = None

    async def connect(self, **kwargs: Any) -> None:
        try:
            self.connection = await aio_pika.connect_robust(self.amqp_url, **kwargs)
            self.channel = await self.connection.channel()
            # Set prefetch count to 1 for fair dispatching if needed
            # await self.channel.set_qos(prefetch_count=1)
            print("RabbitMQ Consumer: Connected and channel established.")
        except Exception as e:
            print(f"RabbitMQ Consumer: Error connecting: {e}")
            raise

    async def disconnect(self) -> None:
        await self.stop_consuming() # Ensure consuming stops before disconnect
        if self.channel:
            await self.channel.close()
            print("RabbitMQ Consumer: Channel closed.")
        if self.connection:
            await self.connection.close()
            print("RabbitMQ Consumer: Connection closed.")
        self.channel = None
        self.connection = None

    async def _process_message(self, incoming_message: aio_pika.IncomingMessage) -> None:
        async with incoming_message.process(): # Acknowledges message upon exiting context
            try:
                body: bytes = incoming_message.body
                # Attempt to decode if headers suggest text, otherwise keep as bytes
                content_type = incoming_message.headers.get('content_type', '') if incoming_message.headers else ''
                if 'text' in content_type or 'json' in content_type or 'xml' in content_type:
                    try:
                        processed_body: str | bytes = body.decode('utf-8')
                    except UnicodeDecodeError:
                        print(f"RabbitMQ Consumer: Could not decode body as UTF-8 despite content_type {content_type}, keeping as bytes.")
                        processed_body = body # Keep as bytes if decoding fails
                else:
                    processed_body = body

                msg_timestamp = incoming_message.timestamp
                if msg_timestamp and isinstance(msg_timestamp, (int, float)):
                     msg_timestamp = datetime.fromtimestamp(msg_timestamp, timezone.utc)
                elif not msg_timestamp:
                    msg_timestamp = datetime.now(timezone.utc)


                message = Message(
                    body=processed_body,
                    headers=dict(incoming_message.headers) if incoming_message.headers else {},
                    message_id=incoming_message.message_id or None, # Use None if not present
                    timestamp=msg_timestamp
                )
                if self._callback:
                    if asyncio.iscoroutinefunction(self._callback):
                        await self._callback(message)
                    else:
                        self._callback(message)
                else:
                    print(f"RabbitMQ Consumer: No callback registered for message {message.id}")
            except Exception as e:
                print(f"RabbitMQ Consumer: Error processing message {incoming_message.message_id}: {e}")
                # Potentially re-queue or move to dead-letter queue depending on strategy
                # For now, message is acked by context manager even on error here.
                # To nack: incoming_message.nack(requeue=False)

    async def subscribe(self, topic: str, callback: Callable[[Message], Any], exchange_name: str = "default_exchange", queue_name: str | None = None, **kwargs: Any) -> None:
        if not self.channel:
            raise ConnectionError("RabbitMQ Consumer is not connected.")

        self._callback = callback
        effective_queue_name = queue_name if queue_name else f"{topic}_queue"

        try:
            # Declare the exchange (idempotent)
            exchange = await self.channel.declare_exchange(
                name=exchange_name,
                type=aio_pika.ExchangeType.DIRECT, # Must match producer
                durable=True,
                **kwargs.get("exchange_declare_kwargs", {})
            )

            # Declare the queue (idempotent)
            self.queue = await self.channel.declare_queue(
                name=effective_queue_name,
                durable=True, # Make queue durable
                **kwargs.get("queue_declare_kwargs", {})
            )

            # Bind the queue to the exchange with the topic as routing key
            await self.queue.bind(exchange, routing_key=topic)
            print(f"RabbitMQ Consumer: Queue '{self.queue.name}' bound to exchange '{exchange_name}' with routing key '{topic}'.")

        except Exception as e:
            print(f"RabbitMQ Consumer: Error subscribing: {e}")
            raise

    async def start_consuming(self) -> None:
        if not self.queue:
            raise RuntimeError("RabbitMQ Consumer: Not subscribed to any queue. Call subscribe() first.")
        if self._consuming_task and not self._consuming_task.done():
            print("RabbitMQ Consumer: Already consuming.")
            return

        print(f"RabbitMQ Consumer: Starting consumption from queue '{self.queue.name}'...")
        try:
            # self._consuming_task = asyncio.create_task(self.queue.consume(self._process_message))
            # The above line creates a task but doesn't keep the consumer alive if the main program exits.
            # For a more robust consumer, it should run in a loop or be awaited.
            # For now, let's make it simple. The caller of start_consuming might need to keep the event loop running.
            await self.queue.consume(self._process_message) # This will block here if not run as a task.
            print(f"RabbitMQ Consumer: Consumption started from queue '{self.queue.name}'.") # This line might not be reached if consume() blocks indefinitely.
        except Exception as e:
            print(f"RabbitMQ Consumer: Error starting consumption: {e}")
            # Reset task if it fails
            self._consuming_task = None
            raise

    async def stop_consuming(self) -> None:
        if self._consuming_task and not self._consuming_task.done():
            print("RabbitMQ Consumer: Stopping consumption...")
            self._consuming_task.cancel()
            try:
                await self._consuming_task
            except asyncio.CancelledError:
                print("RabbitMQ Consumer: Consumption task cancelled.")
            except Exception as e:
                print(f"RabbitMQ Consumer: Error during task cancellation: {e}")
            finally:
                self._consuming_task = None
        else:
            print("RabbitMQ Consumer: No active consumption task to stop.")
        # Note: aio-pika's consume() might not be cancellable this way directly if it's blocking.
        # A more robust stop might involve closing the channel/connection or using a flag.
        # For now, we rely on task cancellation. If `queue.consume` is internally creating a long-lived future,
        # closing the channel might be the more effective way to stop it.
        # Consider using `consumer_tag` for specific cancellation with `queue.cancel()`.
        # For now, this is a simplified stop.
