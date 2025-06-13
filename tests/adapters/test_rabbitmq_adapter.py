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
    unittest.main()
