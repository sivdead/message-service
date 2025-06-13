from .config import settings
from .mq_abstraction_layer import AbstractProducer, AbstractConsumer
from .adapters.rabbitmq import RabbitMQProducer, RabbitMQConsumer
# Import other adapters here as they are added, e.g.:
# from .adapters.kafka import KafkaProducer, KafkaConsumer

class UnsupportedMQAdapterError(ValueError):
    pass

def create_producer() -> AbstractProducer:
    """
    Creates and returns a message queue producer instance based on the configured adapter.
    
    Raises:
        ValueError: If required configuration for the selected adapter is missing.
        UnsupportedMQAdapterError: If the configured adapter is not supported.
    
    Returns:
        An instance of AbstractProducer for the configured message queue adapter.
    """
    if settings.mq_adapter == "rabbitmq":
        if not settings.mq_url:
            raise ValueError("MQ_URL must be set for RabbitMQ adapter.")
        return RabbitMQProducer(amqp_url=settings.mq_url)
    # elif settings.mq_adapter == "kafka":
    #     if not settings.mq_url: # Or specific Kafka brokers setting
    #         raise ValueError("Kafka connection details must be set.")
    #     return KafkaProducer(bootstrap_servers=settings.mq_url)
    else:
        raise UnsupportedMQAdapterError(f"Unsupported MQ adapter: {settings.mq_adapter}")

def create_consumer() -> AbstractConsumer:
    """
    Creates and returns a message queue consumer instance based on the configured adapter.
    
    Raises:
        ValueError: If required configuration for the selected adapter is missing.
        UnsupportedMQAdapterError: If the specified MQ adapter is not supported.
    
    Returns:
        An instance of AbstractConsumer for the configured message queue adapter.
    """
    if settings.mq_adapter == "rabbitmq":
        if not settings.mq_url:
            raise ValueError("MQ_URL must be set for RabbitMQ adapter.")
        return RabbitMQConsumer(amqp_url=settings.mq_url)
    # elif settings.mq_adapter == "kafka":
    #     if not settings.mq_url: # Or specific Kafka brokers setting
    #         raise ValueError("Kafka connection details must be set.")
    #     return KafkaConsumer(bootstrap_servers=settings.mq_url)
    else:
        raise UnsupportedMQAdapterError(f"Unsupported MQ adapter: {settings.mq_adapter}")

if __name__ == '__main__':
    # Example of how to use the factory (requires a running MQ instance for connect)
    print(f"Attempting to create producer for adapter: {settings.mq_adapter}")
    try:
        producer = create_producer()
        print(f"Successfully created producer: {type(producer).__name__}")
    except Exception as e:
        print(f"Error creating producer: {e}")

    print(f"Attempting to create consumer for adapter: {settings.mq_adapter}")
    try:
        consumer = create_consumer()
        print(f"Successfully created consumer: {type(consumer).__name__}")
    except Exception as e:
        print(f"Error creating consumer: {e}")
