import asyncio
import logging
from src import create_producer, create_consumer, settings, Message # Assuming src is in PYTHONPATH

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Shared event to signal when the message has been received
message_received_event = asyncio.Event()
received_message_payload = None

async def message_handler(message: Message) -> None:
    """
    Handles an incoming message by logging its details and signaling receipt.
    
    Stores the message body in a global variable and sets an event to notify that a message has been received.
    """
    global received_message_payload
    logging.info(f"Consumer: Message received! ID: {message.id}, Body: {message.body!r}, Headers: {message.headers}")
    received_message_payload = message.body
    message_received_event.set() # Signal that the message has been received

async def main():
    """
    Coordinates an end-to-end asynchronous message exchange using a producer and consumer.
    
    Initializes and connects a message producer and consumer, subscribes the consumer to a topic, and publishes a test message. Waits for the message to be received and verifies its content. Handles connection setup, message publishing, receipt verification, and orderly cleanup of resources, including cancellation of background tasks and disconnection of clients. Logs progress and errors throughout the process.
    """
    logging.info(f"Starting example with adapter: {settings.mq_adapter}")
    logging.info(f"MQ URL: {settings.mq_url}")
    logging.info(f"Default Topic: {settings.mq_default_topic}")

    producer = None
    consumer = None

    # Use the default topic from settings or a specific one for the example
    example_topic = settings.mq_default_topic or "my_example_topic"
    # For RabbitMQ, we also need an exchange name. Let's use a default one.
    # The producer and consumer in RabbitMQ need to agree on this.
    # Our RabbitMQ adapter implementation uses "default_exchange" if not specified.
    exchange_name = "default_exchange"

    try:
        # Create producer and consumer from the factory
        producer = create_producer()
        consumer = create_consumer()

        # Connect producer and consumer
        logging.info("Producer: Connecting...")
        await producer.connect()
        logging.info("Producer: Connected.")

        logging.info("Consumer: Connecting...")
        await consumer.connect()
        logging.info("Consumer: Connected.")

        # Subscribe the consumer
        # For RabbitMQ, 'topic' acts as routing_key, and we need exchange_name
        logging.info(f"Consumer: Subscribing to topic '{example_topic}' on exchange '{exchange_name}'...")
        await consumer.subscribe(
            topic=example_topic,
            callback=message_handler,
            exchange_name=exchange_name, # Specific to RabbitMQ adapter's needs
            queue_name=f"{example_topic}_queue_example" # Giving a specific queue name for clarity
        )
        logging.info("Consumer: Subscribed.")

        # Start consumer in a background task
        # The RabbitMQConsumer.start_consuming() as implemented might block if not handled as a task.
        # However, for aio-pika, queue.consume() itself starts listening in the background
        # and doesn't necessarily need to be wrapped in asyncio.create_task explicitly
        # if the event loop is managed correctly.
        # Let's ensure it runs concurrently.
        logging.info("Consumer: Starting consumption...")
        # The current RabbitMQConsumer.start_consuming() calls `await self.queue.consume(...)`
        # which might block. Let's run it as a task to allow producer to run.
        consumer_task = asyncio.create_task(consumer.start_consuming())
        logging.info("Consumer: Consumption process initiated.")

        # Allow some time for the consumer to be ready (optional, but good for robustness)
        await asyncio.sleep(1)

        # Create and publish a message
        test_message_body = f"Hello from the example app at {asyncio.get_event_loop().time()}!"
        msg_to_send = Message(body=test_message_body, headers={"source": "simple_usage_example"})

        logging.info(f"Producer: Publishing message ID {msg_to_send.id} to topic '{example_topic}' on exchange '{exchange_name}'...")
        # For RabbitMQ, 'topic' is the routing_key.
        await producer.publish_message(msg_to_send, topic=example_topic, exchange_name=exchange_name)
        logging.info("Producer: Message published.")

        # Wait for the message to be received by the consumer, with a timeout
        try:
            logging.info("Main: Waiting for message to be received by consumer...")
            await asyncio.wait_for(message_received_event.wait(), timeout=10.0)
            logging.info(f"Main: Message successfully received by consumer. Payload: {received_message_payload}")
            assert received_message_payload == test_message_body
            logging.info("Main: Message content assertion passed!")
        except asyncio.TimeoutError:
            logging.error("Main: Timeout! Message was not received by the consumer.")
        except AssertionError:
            logging.error(f"Main: Message content assertion failed! Expected '{test_message_body}', Got '{received_message_payload}'")


    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        logging.info("Cleaning up...")
        if consumer:
            logging.info("Consumer: Stopping consumption...")
            # Stop consuming (implementation might vary in effectiveness based on adapter)
            # For RabbitMQ, cancelling the task and then disconnect should work.
            if 'consumer_task' in locals() and consumer_task and not consumer_task.done():
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    logging.info("Consumer task cancelled successfully.")
                except Exception as e_task:
                    logging.error(f"Error during consumer task cleanup: {e_task}")

            logging.info("Consumer: Disconnecting...")
            await consumer.disconnect()
            logging.info("Consumer: Disconnected.")
        if producer:
            logging.info("Producer: Disconnecting...")
            await producer.disconnect()
            logging.info("Producer: Disconnected.")
        logging.info("Cleanup finished.")

if __name__ == "__main__":
    # Ensure .env is loaded if running this script directly and src is in PYTHONPATH
    # This is already handled by src.config when imported.
    # To run this example:
    # 1. Make sure 'src' is in your PYTHONPATH: export PYTHONPATH=$(pwd):$PYTHONPATH
    # 2. Ensure RabbitMQ (or configured MQ) is running.
    # 3. Run: python examples/simple_usage.py
    # You can create a .env file in the project root to configure MQ_URL etc.
    # Example .env:
    # MQ_ADAPTER="rabbitmq"
    # MQ_URL="amqp://guest:guest@localhost:5672/"
    # MQ_DEFAULT_TOPIC="my_test_topic"

    asyncio.run(main())
