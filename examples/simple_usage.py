import asyncio
import logging
from src import create_producer, create_consumer, settings, Message # Assuming src is in PYTHONPATH

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')

async def demo_delayed_messages():
    """
    Demonstrates sending and receiving a delayed message using RabbitMQ
    with the x-delayed-message plugin.
    """
    logging.info("--- Starting Delayed Message Demo ---")
    # RabbitMQ server must have the 'rabbitmq-delayed-message-exchange' plugin enabled.
    # Example: `docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
    #           -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest \
    #           rabbitmq:3-management sh -c "rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange && rabbitmq-server"`
    logging.info("Prerequisite: RabbitMQ server must have the 'rabbitmq-delayed-message-exchange' plugin enabled.")

    producer = None
    consumer = None
    consumer_task = None

    delayed_exchange = "my_delayed_exchange"
    delayed_topic = "my_delayed_routing_key" # Acts as routing key for the underlying 'direct' exchange
    delayed_queue = "my_delayed_queue_example"
    delay_ms = 5000

    async def delayed_message_handler(message: Message) -> None:
        logging.info(f"Delayed Consumer: Message received! ID: {message.id}, Body: {message.body!r}, Headers: {message.headers}, Delay: {message.delay}ms")
        # Add event set or other synchronization if needed for tests

    try:
        producer = create_producer()
        consumer = create_consumer()

        logging.info("Producer: Connecting...")
        await producer.connect()
        logging.info("Producer: Connected.")

        logging.info("Consumer: Connecting...")
        await consumer.connect()
        logging.info("Consumer: Connected.")

        # Consumer subscribes first
        logging.info(f"Consumer: Subscribing to topic '{delayed_topic}' on delayed exchange '{delayed_exchange}'")
        await consumer.subscribe(
            topic=delayed_topic, # Routing key for the underlying exchange
            callback=delayed_message_handler,
            exchange_name=delayed_exchange,
            queue_name=delayed_queue,
            exchange_type="x-delayed-message",
            # These arguments are for the x-delayed-message exchange itself
            exchange_declare_kwargs={"arguments": {"x-delayed-type": "direct"}} # The underlying exchange type after delay
        )
        logging.info("Consumer: Subscribed.")

        logging.info("Consumer: Starting consumption...")
        consumer_task = asyncio.create_task(consumer.start_consuming())
        logging.info("Consumer: Consumption process initiated.")
        await asyncio.sleep(0.1) # Give consumer a moment to start

        # Producer sends a delayed message
        msg_to_send = Message(
            body=f"Hello, this message was delayed by {delay_ms}ms!",
            headers={"source": "delayed_message_demo"},
            delay=delay_ms # The crucial part: setting the delay on the Message object
        )
        logging.info(f"Producer: Publishing delayed message ID {msg_to_send.id} (delay: {msg_to_send.delay}ms)...")
        await producer.publish_message(
            msg_to_send,
            topic=delayed_topic, # Routing key
            exchange_name=delayed_exchange,
            exchange_type="x-delayed-message", # Must match consumer's exchange type
             # exchange_declare_kwargs are also needed by producer to correctly declare the exchange if it's the first
            exchange_declare_kwargs={"arguments": {"x-delayed-type": "direct"}}
        )
        logging.info("Producer: Delayed message published.")

        logging.info(f"Main: Waiting for {delay_ms/1000 + 2} seconds for delayed message to arrive...")
        await asyncio.sleep(delay_ms / 1000 + 2) # Wait for delay + buffer

    except Exception as e:
        logging.error(f"Delayed Demo Error: {e}", exc_info=True)
    finally:
        logging.info("Delayed Demo: Cleaning up...")
        if consumer_task and not consumer_task.done():
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                logging.info("Delayed Consumer task cancelled.")
        if consumer:
            await consumer.disconnect()
        if producer:
            await producer.disconnect()
        logging.info("--- Delayed Message Demo Finished ---")


async def demo_broadcast_messages():
    """
    Demonstrates sending a broadcast (fanout) message to multiple consumers.
    """
    logging.info("--- Starting Broadcast (Fanout) Message Demo ---")

    producer = None
    consumer1 = None
    consumer2 = None
    consumer1_task = None
    consumer2_task = None

    fanout_exchange = "my_fanout_exchange"
    # Topic/routing key is typically ignored by fanout exchanges, but queue names must be unique for multiple consumers.
    # We still need to provide a topic to `publish_message` and `subscribe` as per method signatures.
    # The `subscribe` method will use it for default queue name generation if not provided.
    # For fanout, the routing_key argument in queue.bind() is often an empty string.
    fanout_topic_placeholder = "broadcast_info" # Used for queue naming, not routing by fanout

    async def broadcast_message_handler_1(message: Message) -> None:
        logging.info(f"Broadcast Consumer 1: Message received! ID: {message.id}, Body: {message.body!r}")

    async def broadcast_message_handler_2(message: Message) -> None:
        logging.info(f"Broadcast Consumer 2: Message received! ID: {message.id}, Body: {message.body!r}")

    try:
        producer = create_producer()
        consumer1 = create_consumer()
        consumer2 = create_consumer()

        # Connect all
        await producer.connect()
        logging.info("Producer: Connected.")
        await consumer1.connect()
        logging.info("Consumer 1: Connected.")
        await consumer2.connect()
        logging.info("Consumer 2: Connected.")

        # Consumers subscribe to the same fanout exchange but with different queues
        # Note: For fanout, routing_key for binding is usually empty. Our adapter handles this.
        # Queue names MUST be different for each consumer to get a copy of the message.
        # If queue_name is not specified, our consumer's subscribe method should generate unique enough names
        # (e.g., f"{topic}_{exchange_type}_queue"), but explicit names are clearer here.

        queue_name1 = f"{fanout_topic_placeholder}_q1_fanout"
        logging.info(f"Consumer 1: Subscribing to fanout exchange '{fanout_exchange}', queue '{queue_name1}'")
        await consumer1.subscribe(
            topic=fanout_topic_placeholder, # Placeholder, actual routing determined by fanout
            callback=broadcast_message_handler_1,
            exchange_name=fanout_exchange,
            exchange_type="fanout",
            queue_name=queue_name1
        )
        logging.info("Consumer 1: Subscribed.")

        queue_name2 = f"{fanout_topic_placeholder}_q2_fanout"
        logging.info(f"Consumer 2: Subscribing to fanout exchange '{fanout_exchange}', queue '{queue_name2}'")
        await consumer2.subscribe(
            topic=fanout_topic_placeholder, # Placeholder
            callback=broadcast_message_handler_2,
            exchange_name=fanout_exchange,
            exchange_type="fanout",
            queue_name=queue_name2
        )
        logging.info("Consumer 2: Subscribed.")

        # Start consumers
        consumer1_task = asyncio.create_task(consumer1.start_consuming())
        consumer2_task = asyncio.create_task(consumer2.start_consuming())
        logging.info("Consumers: Consumption processes initiated.")
        await asyncio.sleep(0.1) # Give consumers a moment

        # Producer sends a broadcast message
        msg_to_send = Message(body="Hello all, this is a broadcast!", headers={"source": "broadcast_demo"})
        logging.info(f"Producer: Publishing broadcast message ID {msg_to_send.id} to fanout exchange '{fanout_exchange}'...")
        # For fanout, the routing_key in publish is often ignored by the broker, but the method might require it.
        # Our producer's publish_message uses topic as routing_key if routing_key param is None.
        # The consumer's subscribe method sets binding key to "" for fanout.
        await producer.publish_message(
            msg_to_send,
            topic=fanout_topic_placeholder, # This will be the routing_key if not specified otherwise
            exchange_name=fanout_exchange,
            exchange_type="fanout"
        )
        logging.info("Producer: Broadcast message published.")

        logging.info("Main: Waiting for 2 seconds for broadcast messages to be processed...")
        await asyncio.sleep(2)

    except Exception as e:
        logging.error(f"Broadcast Demo Error: {e}", exc_info=True)
    finally:
        logging.info("Broadcast Demo: Cleaning up...")
        if consumer1_task and not consumer1_task.done():
            consumer1_task.cancel()
            try: await consumer1_task
            except asyncio.CancelledError: logging.info("Broadcast Consumer 1 task cancelled.")
        if consumer2_task and not consumer2_task.done():
            consumer2_task.cancel()
            try: await consumer2_task
            except asyncio.CancelledError: logging.info("Broadcast Consumer 2 task cancelled.")

        if consumer1: await consumer1.disconnect()
        if consumer2: await consumer2.disconnect()
        if producer: await producer.disconnect()
        logging.info("--- Broadcast (Fanout) Message Demo Finished ---")

async def main():
    """
    Main function to run messaging demonstrations.
    """
    logging.info(f"Starting example with adapter: {settings.mq_adapter}")
    logging.info(f"MQ URL: {settings.mq_url}")

    if settings.mq_adapter == "rabbitmq":
        await demo_delayed_messages()
        await asyncio.sleep(2) # Pause between demos
        await demo_broadcast_messages()
    else:
        logging.warning(f"Adapter '{settings.mq_adapter}' does not support all demo features. Skipping advanced demos.")
        # Optionally, run the original simple_usage logic here for other adapters
        logging.info("Running a simple point-to-point message test (original demo logic).")
        # (Original simple_usage main logic could be refactored into a function and called here)
        # For now, just logging this.
        # This part is not implemented to run the original demo logic.
        # You would need to copy-paste or refactor the original main() content here.


if __name__ == "__main__":
    # To run this example:
    # 1. Make sure 'src' is in your PYTHONPATH: export PYTHONPATH=$(pwd):$PYTHONPATH
    # 2. Ensure RabbitMQ (or configured MQ) is running.
    #    For delayed messages, RabbitMQ needs the 'rabbitmq-delayed-message-exchange' plugin.
    # 3. Run: python examples/simple_usage.py
    #
    # Example .env in project root:
    # MQ_ADAPTER="rabbitmq"
    # MQ_URL="amqp://guest:guest@localhost:5672/"
    # MQ_DEFAULT_TOPIC="default_topic_not_used_by_demos"

    asyncio.run(main())
