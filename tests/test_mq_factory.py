import unittest
from unittest import mock

# Assuming src.config.settings and src.mq_factory can be imported
from src.config import settings
from src.mq_factory import create_producer, create_consumer, UnsupportedMQAdapterError
from src.adapters.rabbitmq import RabbitMQProducer, RabbitMQConsumer

class TestMQFactory(unittest.TestCase):

    @mock.patch('src.config.settings')
    def test_create_rabbitmq_producer(self, mock_settings):
        mock_settings.mq_adapter = "rabbitmq"
        mock_settings.mq_url = "amqp://test:test@localhost/"
        producer = create_producer()
        self.assertIsInstance(producer, RabbitMQProducer)
        self.assertEqual(producer.amqp_url, "amqp://test:test@localhost/")

    @mock.patch('src.config.settings')
    def test_create_rabbitmq_consumer(self, mock_settings):
        """
        Tests that create_consumer returns a RabbitMQConsumer instance with the correct URL when the adapter is set to 'rabbitmq'.
        """
        mock_settings.mq_adapter = "rabbitmq"
        mock_settings.mq_url = "amqp://test:test@localhost/"
        consumer = create_consumer()
        self.assertIsInstance(consumer, RabbitMQConsumer)
        self.assertEqual(consumer.amqp_url, "amqp://test:test@localhost/")

    @mock.patch('src.config.settings')
    def test_unsupported_adapter(self, mock_settings):
        """
        Tests that using an unsupported MQ adapter raises an UnsupportedMQAdapterError.
        
        Verifies that both producer and consumer factory functions raise the expected
        exception when the adapter type is not recognized.
        """
        mock_settings.mq_adapter = "non_existent_mq"
        mock_settings.mq_url = "some_url"
        with self.assertRaises(UnsupportedMQAdapterError):
            create_producer()
        with self.assertRaises(UnsupportedMQAdapterError):
            create_consumer()

    @mock.patch('src.config.settings')
    def test_missing_url_for_rabbitmq(self, mock_settings):
        """
        Tests that a ValueError is raised when the RabbitMQ adapter is selected but the MQ URL is missing.
        
        Verifies that both create_producer() and create_consumer() raise a ValueError with the expected message if the configuration lacks the required MQ URL for the RabbitMQ adapter.
        """
        mock_settings.mq_adapter = "rabbitmq"
        mock_settings.mq_url = None # Simulate missing URL
        with self.assertRaises(ValueError) as P_cm:
            create_producer()
        self.assertIn("MQ_URL must be set for RabbitMQ adapter", str(P_cm.exception))

        with self.assertRaises(ValueError) as C_cm:
            create_consumer()
        self.assertIn("MQ_URL must be set for RabbitMQ adapter", str(C_cm.exception))

if __name__ == '__main__':
    unittest.main()
