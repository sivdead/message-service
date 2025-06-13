import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
# Useful for development environments
load_dotenv()

class AppConfig:
    def __init__(self):
        """
        Initializes MQ configuration settings from environment variables with default values.
        
        Reads the MQ adapter type, connection URL, and default topic from environment variables,
        falling back to sensible defaults if not set.
        """
        self.mq_adapter: str | None = os.getenv("MQ_ADAPTER", "rabbitmq") # Default to rabbitmq
        self.mq_url: str | None = os.getenv("MQ_URL", "amqp://guest:guest@localhost:5672/") # Default RabbitMQ URL
        self.mq_default_topic: str | None = os.getenv("MQ_DEFAULT_TOPIC", "default_topic")
        # Add other general MQ settings here if needed

    def __repr__(self) -> str:
        """
        Returns a string representation of the AppConfig instance, showing current MQ settings.
        """
        return (
            f"AppConfig(mq_adapter='{self.mq_adapter}', mq_url='{self.mq_url}', "
            f"mq_default_topic='{self.mq_default_topic}')"
        )

# Global instance of the config that can be imported
settings = AppConfig()

if __name__ == '__main__':
    # Example of how to use it
    print("Current Configuration:")
    print(f"  Adapter: {settings.mq_adapter}")
    print(f"  URL: {settings.mq_url}")
    print(f"  Default Topic: {settings.mq_default_topic}")

    # To test with a .env file, you would create a .env file in the project root:
    # MQ_ADAPTER="custom_adapter"
    # MQ_URL="custom_url"
    # MQ_DEFAULT_TOPIC="another_topic"
