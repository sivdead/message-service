import unittest
import asyncio
from unittest import mock # unittest.mock, not just mock if Python > 3.3
from src.adapters.rabbitmq import RabbitMQProducer, RabbitMQConsumer
from src.unified_message_model import Message
from datetime import datetime, timezone

# Helper to run async tests in unittest until Python 3.8+ (which has better built-in support)
# For older versions, a library like 'asynctest' would be helpful.
# Here, we'll use asyncio.run for individual async test methods if needed,
# or structure tests to use a single event loop.

def async_test(coro):
    """
    Decorator to run an asynchronous test coroutine in a new event loop.
    
    Ensures that each decorated test function executes in its own isolated asyncio event loop,
    allowing asynchronous tests to be run within synchronous test frameworks like unittest.
    """
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro(*args, **kwargs))
        finally:
            loop.close()
            asyncio.set_event_loop(None) # Clean up to avoid interference
    return wrapper

class TestRabbitMQProducer(unittest.TestCase):
    TEST_AMQP_URL = "amqp://mock:mock@localhost/mockvhost"

    @mock.patch('aio_pika.connect_robust')
    @async_test
    async def test_producer_connect_disconnect(self, mock_connect_robust):
        """
        Tests that RabbitMQProducer connects and disconnects properly, establishing and closing connections and channels as expected.
        """
        mock_connection = mock.AsyncMock()
        mock_channel = mock.AsyncMock()
        mock_connect_robust.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        producer = RabbitMQProducer(self.TEST_AMQP_URL)
        await producer.connect()

        mock_connect_robust.assert_called_once_with(self.TEST_AMQP_URL)
        mock_connection.channel.assert_called_once()
        self.assertIsNotNone(producer.connection)
        self.assertIsNotNone(producer.channel)

        await producer.disconnect()
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        self.assertIsNone(producer.channel)
        self.assertIsNone(producer.connection)

    @mock.patch('aio_pika.connect_robust')
    @async_test
    async def test_producer_publish_message(self, mock_connect_robust):
        """
        Tests that RabbitMQProducer publishes a message with correct properties to a custom exchange and routing key.
        
        Verifies that the exchange is declared, the message is published with the expected body, message ID, headers, and delivery mode, and that the correct routing key is used.
        """
        mock_connection = mock.AsyncMock()
        mock_channel = mock.AsyncMock()
        mock_exchange = mock.AsyncMock()

        mock_connect_robust.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange

        producer = RabbitMQProducer(self.TEST_AMQP_URL)
        await producer.connect()

        test_message = Message(body="Test Body", message_id="test-id", headers={"X-Test": "true"})
        topic = "test_topic"
        exchange_name = "custom_exchange"

        await producer.publish_message(test_message, topic=topic, exchange_name=exchange_name, routing_key="custom_routing_key")

        mock_channel.declare_exchange.assert_called_once_with(
            name=exchange_name, type=mock.ANY, durable=True, exchange_declare_kwargs={}
        )
        mock_exchange.publish.assert_called_once()

        # Check the properties of the aio_pika.Message
        args, kwargs = mock_exchange.publish.call_args
        sent_aio_message = args[0]
        self.assertEqual(sent_aio_message.body, b"Test Body") # Assumes UTF-8 encoding
        self.assertEqual(sent_aio_message.message_id, "test-id")
        self.assertEqual(sent_aio_message.headers, {"X-Test": "true"})
        self.assertEqual(sent_aio_message.delivery_mode, mock.ANY) # aio_pika.DeliveryMode.PERSISTENT

        self.assertEqual(kwargs['routing_key'], "custom_routing_key")

        await producer.disconnect()

    @async_test
    async def test_publish_not_connected(self):
        """
        Tests that publishing a message without an active connection raises a ConnectionError.
        """
        producer = RabbitMQProducer(self.TEST_AMQP_URL)
        test_message = Message(body="Test")
        with self.assertRaises(ConnectionError):
            await producer.publish_message(test_message, topic="test")


class TestRabbitMQConsumer(unittest.TestCase):
    TEST_AMQP_URL = "amqp://mock:mock@localhost/mockvhost"

    @mock.patch('aio_pika.connect_robust')
    @async_test
    async def test_consumer_connect_disconnect(self, mock_connect_robust):
        """
        Tests that RabbitMQConsumer connects and disconnects properly, establishing and closing the connection and channel as expected.
        """
        mock_connection = mock.AsyncMock()
        mock_channel = mock.AsyncMock()
        mock_connect_robust.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        consumer = RabbitMQConsumer(self.TEST_AMQP_URL)
        await consumer.connect()

        mock_connect_robust.assert_called_once_with(self.TEST_AMQP_URL)
        mock_connection.channel.assert_called_once()
        self.assertIsNotNone(consumer.connection)
        self.assertIsNotNone(consumer.channel)

        await consumer.disconnect() # This also calls stop_consuming, which is fine
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        self.assertIsNone(consumer.channel)
        self.assertIsNone(consumer.connection)

    @mock.patch('aio_pika.connect_robust')
    @async_test
    async def test_consumer_subscribe(self, mock_connect_robust):
        """
        Tests that RabbitMQConsumer can subscribe to a topic by declaring the exchange and queue,
        binding the queue to the exchange, and setting the callback function.
        
        Verifies that the correct exchange and queue declarations are made, the queue is bound
        with the specified routing key, and the consumer's internal state is updated accordingly.
        """
        mock_connection = mock.AsyncMock()
        mock_channel = mock.AsyncMock()
        mock_exchange = mock.AsyncMock()
        mock_queue = mock.AsyncMock()

        mock_connect_robust.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_exchange.return_value = mock_exchange
        mock_channel.declare_queue.return_value = mock_queue
        mock_queue.name = "mock_queue_name" # Set the name attribute for the mock queue

        consumer = RabbitMQConsumer(self.TEST_AMQP_URL)
        await consumer.connect()

        topic = "sub_topic"
        exchange_name = "sub_exchange"
        queue_name = "sub_queue"
        mock_callback = mock.Mock()

        await consumer.subscribe(
            topic=topic,
            callback=mock_callback,
            exchange_name=exchange_name,
            queue_name=queue_name
        )

        mock_channel.declare_exchange.assert_called_once_with(
            name=exchange_name, type=mock.ANY, durable=True, exchange_declare_kwargs={}
        )
        mock_channel.declare_queue.assert_called_once_with(
            name=queue_name, durable=True, queue_declare_kwargs={}
        )
        mock_queue.bind.assert_called_once_with(mock_exchange, routing_key=topic)
        self.assertEqual(consumer._callback, mock_callback)
        self.assertEqual(consumer.queue, mock_queue)

        await consumer.disconnect()

    @mock.patch('aio_pika.connect_robust')
    @async_test
    async def test_process_message_text(self, mock_connect_robust):
        # This test is more involved as it simulates receiving a message
        """
        Tests that the consumer correctly processes an incoming JSON message and invokes the callback with a decoded Message instance.
        
        Simulates receiving an aio_pika.IncomingMessage with JSON content, verifies that the message body is decoded to a string, metadata is preserved, and the message is acknowledged.
        """
        consumer = RabbitMQConsumer(self.TEST_AMQP_URL)
        consumer._callback = mock.AsyncMock() # Use AsyncMock for async callback

        mock_incoming_message = mock.AsyncMock(spec=aio_pika.IncomingMessage)
        mock_incoming_message.body = b'{"key": "value"}'
        mock_incoming_message.headers = {'content_type': 'application/json'}
        mock_incoming_message.message_id = "msg-json-id"
        mock_incoming_message.timestamp = datetime.now(timezone.utc)

        await consumer._process_message(mock_incoming_message)

        consumer._callback.assert_called_once()
        args, _ = consumer._callback.call_args
        received_msg: Message = args[0]

        self.assertEqual(received_msg.body, '{"key": "value"}') # Decoded as string
        self.assertEqual(received_msg.id, "msg-json-id")
        self.assertEqual(received_msg.headers, {'content_type': 'application/json'})
        mock_incoming_message.process.assert_called_once() # Check ack

    @mock.patch('aio_pika.connect_robust')
    @async_test
    async def test_process_message_binary(self, mock_connect_robust):
        """
        Tests that the consumer correctly processes and delivers a binary message.
        
        Verifies that a binary message received by the consumer is passed to the callback with the body preserved as bytes, the message ID set, and the timestamp converted to a datetime object.
        """
        consumer = RabbitMQConsumer(self.TEST_AMQP_URL)
        consumer._callback = mock.Mock() # Regular mock for sync callback

        mock_incoming_message = mock.AsyncMock(spec=aio_pika.IncomingMessage)
        mock_incoming_message.body = b'\xde\xad\xbe\xef'
        mock_incoming_message.headers = {'content_type': 'application/octet-stream'}
        mock_incoming_message.message_id = "msg-bin-id"
        mock_incoming_message.timestamp = int(datetime.now(timezone.utc).timestamp())


        await consumer._process_message(mock_incoming_message)
        consumer._callback.assert_called_once()
        args, _ = consumer._callback.call_args
        received_msg: Message = args[0]

        self.assertEqual(received_msg.body, b'\xde\xad\xbe\xef') # Remains bytes
        self.assertEqual(received_msg.id, "msg-bin-id")
        self.assertIsInstance(received_msg.timestamp, datetime)


    @async_test
    async def test_subscribe_not_connected(self):
        """
        Tests that subscribing without an active connection raises a ConnectionError.
        """
        consumer = RabbitMQConsumer(self.TEST_AMQP_URL)
        with self.assertRaises(ConnectionError):
            await consumer.subscribe(topic="test", callback=mock.Mock())

    @async_test
    async def test_start_consuming_not_subscribed(self):
        """
        Tests that starting consumption without a prior queue subscription raises a RuntimeError.
        
        Verifies that attempting to call `start_consuming` on a connected `RabbitMQConsumer` instance without subscribing to a queue results in a RuntimeError with an appropriate error message.
        """
        consumer = RabbitMQConsumer(self.TEST_AMQP_URL)
        # Simulate connected state without actual connection
        consumer.channel = mock.AsyncMock()
        with self.assertRaises(RuntimeError) as cm:
            await consumer.start_consuming()
        self.assertIn("Not subscribed to any queue", str(cm.exception))


if __name__ == '__main__':
    # This allows running tests for this file directly.
    # For all tests, use 'python -m unittest discover tests' from root.
    # unittest.main() # Comment out to allow pytest to run if this file is discovered by pytest

# Pytest-style tests for new functionality (Delayed & Broadcast)
# These are integration tests and require a running RabbitMQ instance.

import pytest
import pytest_asyncio # For async fixtures if needed, though direct setup used here
import uuid
import os
import time # For checking message arrival times
from asyncio import Queue as AsyncQueue # Explicitly use asyncio.Queue
from src.adapters.rabbitmq import RabbitMQProducer, RabbitMQConsumer # Assuming these are the correct paths
from src.unified_message_model import Message # Assuming this is the correct path
import aio_pika # For exceptions and ExchangeType

# AMQP URL for tests - ensure your test RabbitMQ is accessible here
# For local testing, often "amqp://guest:guest@localhost/"
# For CI, this might be set via environment variables.
TEST_AMQP_URL_PYTEST = os.environ.get("MQ_URL_TEST", "amqp://guest:guest@localhost/")

def generate_unique_name(base_name: str) -> str:
    """Generates a unique name by appending a UUID short hex."""
    return f"{base_name}_{uuid.uuid4().hex[:8]}"

@pytest.mark.asyncio
async def test_delayed_message_rabbitmq():
    """
    Tests that a message sent with a delay is received after that delay.
    Requires RabbitMQ server with the 'rabbitmq-delayed-message-exchange' plugin enabled.
    """
    producer = RabbitMQProducer(TEST_AMQP_URL_PYTEST)
    consumer = RabbitMQConsumer(TEST_AMQP_URL_PYTEST)

    exchange_name = generate_unique_name("test_delayed_exchange")
    routing_key = generate_unique_name("delayed_key") # Also acts as queue name part
    queue_name = f"{routing_key}_delayed_q"
    delay_ms = 2000  # 2 seconds
    tolerance_s = 1.0 # Allow 1s tolerance for processing, network, etc.

    received_messages_queue = AsyncQueue()

    async def message_handler(message: Message):
        await received_messages_queue.put(message)
        print(f"Delayed test: Received message {message.id} at {time.time()}")

    try:
        await producer.connect()
        await consumer.connect()

        # Attempt to declare the delayed exchange; skip test if plugin is not enabled
        try:
            await consumer.channel.declare_exchange(
                name=exchange_name,
                type="x-delayed-message",
                durable=False, # Non-durable for testing
                auto_delete=True, # Auto-delete for testing
                arguments={"x-delayed-type": "direct"}
            )
            # Also declare on producer's channel if it's a different channel object (it should be)
            await producer.channel.declare_exchange(
                name=exchange_name,
                type="x-delayed-message",
                durable=False,
                auto_delete=True,
                arguments={"x-delayed-type": "direct"}
            )
        except aio_pika.exceptions.ChannelClosedByBroker as e:
            # 503: COMMAND_INVALID (exchange type not found)
            # 541: INTERNAL_ERROR (sometimes for plugin issues)
            if e.reply_code == 503 or "exchange type 'x-delayed-message' not found" in str(e):
                pytest.skip("RabbitMQ delayed message plugin not enabled or x-delayed-message type not found.")
            raise

        await consumer.subscribe(
            topic=routing_key,
            callback=message_handler,
            exchange_name=exchange_name,
            queue_name=queue_name,
            exchange_type="x-delayed-message", # Consumer needs to know this
            exchange_declare_kwargs={ # Consumer also declares, needs these args
                "durable": False, "auto_delete": True, "arguments": {"x-delayed-type": "direct"}
            },
            queue_declare_kwargs={"durable": False, "auto_delete": True}
        )

        consumer_task = asyncio.create_task(consumer.start_consuming())
        await asyncio.sleep(0.1) # Allow consumer to start

        msg_body = f"Delayed test message, delay {delay_ms}ms"
        msg_to_send = Message(body=msg_body, delay=delay_ms)

        start_time = time.time()
        print(f"Delayed test: Publishing message {msg_to_send.id} with {delay_ms}ms delay at {start_time}")
        await producer.publish_message(
            msg_to_send,
            topic=routing_key,
            exchange_name=exchange_name,
            # Producer also needs to know exchange type for declaration if it's the first
            # However, our producer's publish_message now takes exchange_type
            exchange_type="x-delayed-message",
            exchange_declare_kwargs={
                "durable": False, "auto_delete": True, "arguments": {"x-delayed-type": "direct"}
            }
        )

        # 1. Check that message is NOT received almost immediately
        try:
            await asyncio.wait_for(received_messages_queue.get(), timeout=delay_ms / 1000 * 0.5)
            # If we get here, message was received too early
            pytest.fail("Delayed message received sooner than expected.")
        except asyncio.TimeoutError:
            print("Delayed test: Message not received prematurely, as expected.")
            pass # Expected

        # 2. Check that message IS received after the delay
        try:
            print(f"Delayed test: Waiting for message, timeout {(delay_ms / 1000 * 0.5) + tolerance_s + 1}s")
            received_msg = await asyncio.wait_for(received_messages_queue.get(), timeout=(delay_ms / 1000 * 0.5) + tolerance_s + 1) # Remaining delay + tolerance
            end_time = time.time()
            actual_delay_s = end_time - start_time

            print(f"Delayed test: Message received after {actual_delay_s:.2f}s.")
            assert received_msg.body == msg_body
            # Check if actual delay is within expected range (delay_ms/1000 to delay_ms/1000 + tolerance_s)
            # This check can be flaky due to system load, so give generous bounds or focus on "at least delay"
            assert actual_delay_s >= (delay_ms / 1000 * 0.9) # e.g. at least 90% of expected delay
            assert actual_delay_s <= (delay_ms / 1000) + tolerance_s + 1.0 # Upper bound with extra tolerance

        except asyncio.TimeoutError:
            pytest.fail("Delayed message not received within the expected time window.")
        finally:
            if consumer_task and not consumer_task.done():
                consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await consumer_task

    finally:
        if producer: await producer.disconnect()
        if consumer: await consumer.disconnect()
        # Attempt to clean up exchange (if durable=False, auto_delete=True, this might not be strictly needed)
        # cleanup_channel = await consumer.connection.channel() # Requires consumer to be connected
        # if cleanup_channel:
        #     await cleanup_channel.exchange_delete(exchange_name)
        #     await cleanup_channel.close()


@pytest.mark.asyncio
async def test_broadcast_message_rabbitmq():
    """
    Tests that a message sent to a fanout exchange is received by multiple consumers.
    """
    producer = RabbitMQProducer(TEST_AMQP_URL_PYTEST)
    consumer1 = RabbitMQConsumer(TEST_AMQP_URL_PYTEST)
    consumer2 = RabbitMQConsumer(TEST_AMQP_URL_PYTEST)

    exchange_name = generate_unique_name("test_fanout_exchange")
    # For fanout, routing key in publish is ignored, but consumers need unique queue names.
    # The 'topic' parameter in subscribe is used for queue name generation if queue_name is not given.
    topic_placeholder = generate_unique_name("broadcast_topic")

    received_messages_c1 = AsyncQueue()
    received_messages_c2 = AsyncQueue()

    async def handler_c1(message: Message): await received_messages_c1.put(message)
    async def handler_c2(message: Message): await received_messages_c2.put(message)

    consumer1_task = None
    consumer2_task = None

    try:
        await producer.connect()
        await consumer1.connect()
        await consumer2.connect()

        # Consumers subscribe to the same fanout exchange with unique queues
        # Queues should be auto-delete for tests if not explicitly named and cleaned up.
        # Our consumer's subscribe method by default creates durable queues, so specify for tests.
        common_exchange_kwargs = {"durable": False, "auto_delete": True}
        common_queue_kwargs = {"durable": False, "auto_delete": True}

        await consumer1.subscribe(
            topic=topic_placeholder, callback=handler_c1, exchange_name=exchange_name,
            exchange_type="fanout", queue_name=generate_unique_name(f"{topic_placeholder}_q1"),
            exchange_declare_kwargs=common_exchange_kwargs, queue_declare_kwargs=common_queue_kwargs
        )
        consumer1_task = asyncio.create_task(consumer1.start_consuming())

        await consumer2.subscribe(
            topic=topic_placeholder, callback=handler_c2, exchange_name=exchange_name,
            exchange_type="fanout", queue_name=generate_unique_name(f"{topic_placeholder}_q2"),
            exchange_declare_kwargs=common_exchange_kwargs, queue_declare_kwargs=common_queue_kwargs
        )
        consumer2_task = asyncio.create_task(consumer2.start_consuming())

        await asyncio.sleep(0.2) # Allow consumers to start and bind

        msg_body = "Broadcast test message"
        msg_to_send = Message(body=msg_body)

        await producer.publish_message(
            msg_to_send,
            topic=topic_placeholder, # Routing key often ignored by fanout, but method might need it
            exchange_name=exchange_name,
            exchange_type="fanout",
            exchange_declare_kwargs=common_exchange_kwargs
        )

        timeout_s = 5.0
        try:
            msg_c1 = await asyncio.wait_for(received_messages_c1.get(), timeout=timeout_s)
            msg_c2 = await asyncio.wait_for(received_messages_c2.get(), timeout=timeout_s)

            assert msg_c1.body == msg_body
            assert msg_c2.body == msg_body
            assert msg_c1.id == msg_to_send.id # Assuming message ID is preserved
            assert msg_c2.id == msg_to_send.id

            print(f"Broadcast test: Message '{msg_body}' received by both consumers.")
        except asyncio.TimeoutError:
            pytest.fail(f"Broadcast message not received by both consumers within {timeout_s}s.")
        finally:
            if consumer1_task and not consumer1_task.done():
                consumer1_task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await consumer1_task
            if consumer2_task and not consumer2_task.done():
                consumer2_task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await consumer2_task

    finally:
        if producer: await producer.disconnect()
        if consumer1: await consumer1.disconnect()
        if consumer2: await consumer2.disconnect()
        # Exchange should auto-delete if declared with auto_delete=True and durable=False

# To run these pytest tests:
# 1. Ensure RabbitMQ is running and accessible via TEST_AMQP_URL_PYTEST.
# 2. For delayed messages, ensure the 'rabbitmq-delayed-message-exchange' plugin is enabled on the server.
# 3. Install pytest and pytest-asyncio: pip install pytest pytest-asyncio
# 4. Run from the project root: pytest tests/adapters/test_rabbitmq_adapter.py

# Need to import contextlib for suppress
import contextlib

if __name__ == '__main__':
    # This allows running tests for this file directly.
    # For all tests, use 'python -m unittest discover tests' from root
    # or 'pytest'
    # To run only unittests: python -m unittest tests/adapters/test_rabbitmq_adapter.py
    # To run only pytest tests: pytest tests/adapters/test_rabbitmq_adapter.py

    # It's generally better to run pytest from the command line for proper discovery and plugin loading.
    # If you want to run unittests, you might need to uncomment the unittest.main() call
    # and comment out the pytest section or run it selectively.
    pass # Let pytest handle execution primarily
