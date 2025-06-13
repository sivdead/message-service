import unittest
from unittest import mock
import os

# Need to ensure 'src.config' can be imported. If tests are run from root, and src is in PYTHONPATH.
# The AppConfig class and settings instance are in src.config
from src.config import AppConfig

class TestAppConfig(unittest.TestCase):

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_config_defaults(self):
        # Reload AppConfig or create new instance to pick up mocked env
        """
        Tests that AppConfig uses default values when no environment variables are set.
        """
        config = AppConfig()
        self.assertEqual(config.mq_adapter, "rabbitmq")
        self.assertEqual(config.mq_url, "amqp://guest:guest@localhost:5672/")
        self.assertEqual(config.mq_default_topic, "default_topic")

    @mock.patch.dict(os.environ, {
        "MQ_ADAPTER": "test_adapter",
        "MQ_URL": "test_url",
        "MQ_DEFAULT_TOPIC": "test_topic_env"
    }, clear=True)
    def test_config_from_env_variables(self):
        """
        Tests that AppConfig correctly loads values from environment variables.
        
        Ensures that when relevant environment variables are set, the AppConfig instance
        reflects these values in its attributes.
        """
        config = AppConfig()
        self.assertEqual(config.mq_adapter, "test_adapter")
        self.assertEqual(config.mq_url, "test_url")
        self.assertEqual(config.mq_default_topic, "test_topic_env")

    def test_config_representation(self):
        """
        Tests that the string representation of AppConfig includes its key attributes.
        """
        config = AppConfig()
        representation = repr(config)
        self.assertIn("AppConfig(mq_adapter=", representation)
        self.assertIn("mq_url=", representation)
        self.assertIn("mq_default_topic=", representation)

if __name__ == '__main__':
    unittest.main()
