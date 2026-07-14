# Unified Message Queue Library

A Python library for interacting with various message queue middlewares through a unified interface. This library allows developers to write message publishing and consuming logic once and easily switch between different message queue systems by changing configuration.

## Features

- **Unified Message Model**: A consistent `Message` object across different MQ systems.
- **Abstraction Layer**: `AbstractProducer` and `AbstractConsumer` interfaces for common MQ operations.
- **Pluggable Adapters**: Easily extendable with new adapters for different MQ middlewares.
- **Configuration-Driven**: Switch MQ backends by changing environment variables or a `.env` file.
- **Asynchronous**: Built with `asyncio` for modern asynchronous Python applications.

### Advanced Messaging Patterns (RabbitMQ)

The RabbitMQ adapter supports several advanced messaging patterns:

-   **Delayed Messages**:
    -   Messages can be scheduled for delayed delivery by setting the `delay` attribute (in milliseconds) on the `Message` object (e.g., `Message(body="...", delay=5000)`).
    -   To use this feature with RabbitMQ, you must publish to an exchange specifically declared with the type `"x-delayed-message"`.
    -   When declaring such an exchange (usually done by both producer and consumer if they might be the first to declare), you also need to specify the underlying exchange type that will handle the message after the delay. This is done via `exchange_declare_kwargs`. For example: `exchange_declare_kwargs={"arguments": {"x-delayed-type": "direct"}}` would mean that after the delay, the message is routed as if it were on a `direct` exchange.
    -   **Prerequisite**: The RabbitMQ server must have the `rabbitmq-delayed-message-exchange` plugin enabled.

-   **Broadcast Messages (Fanout)**:
    -   To send a message to multiple consumers simultaneously (broadcast), publish to an exchange of type `"fanout"`.
    -   Each consumer interested in these broadcast messages should subscribe to this `fanout` exchange. Crucially, each consumer must bind its own uniquely named queue to the exchange. This ensures each queue gets a copy of the message.

-   **Flexible Exchange Types**:
    -   The `RabbitMQProducer.publish_message()` and `RabbitMQConsumer.subscribe()` methods now include an `exchange_type` parameter (e.g., `exchange_type="direct"`).
    -   This allows explicit control over the type of exchange used, supporting standard types like `direct`, `fanout`, `topic`, `headers`, as well as custom types like `"x-delayed-message"`.

## Current Supported Middlewares

- **RabbitMQ** (via `aio-pika`)

## Project Structure

```
.
├── examples/                # Example usage scripts
│   ├── simple_usage.py
│   └── README.md
├── message-service/         # Source code (formerly src/)
│   ├── adapters/            # Concrete MQ adapter implementations
│   │   └── rabbitmq/        # RabbitMQ specific adapter
│   │       ├── __init__.py
│   │       ├── consumer.py
│   │       └── producer.py
│   ├── mq_abstraction_layer/ # Abstract producer/consumer interfaces
│   │   ├── __init__.py
│   │   ├── consumer.py
│   │   └── producer.py
│   ├── model/               # The generic Message class (formerly unified_message_model/)
│   │   ├── __init__.py
│   │   └── message.py
│   ├── __init__.py          # Makes 'message-service' a package
│   ├── config.py            # Configuration loading (env vars, .env)
│   └── mq_factory.py        # Factory for creating producer/consumer instances
├── tests/                   # Unit and integration tests
│   ├── adapters/
│   │   └── test_rabbitmq_adapter.py
│   ├── __init__.py
│   ├── test_config.py
│   ├── test_message_model.py
│   └── test_mq_factory.py
├── .env.example             # Example .env file
├── .gitignore               # Standard Python .gitignore
├── pyproject.toml           # Project metadata and dependencies (for uv)
├── README.md                # This file
└── run_tests.sh             # Script to run unit tests
```

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

1.  **Clone the repository**:
    ```bash
    git clone <your-repo-url>
    cd <your-repo-name>
    ```

2.  **Install dependencies using uv**:
    If you have `uv` installed:
    ```bash
    uv sync
    ```
    This will create a virtual environment (if one isn't active) and install dependencies listed in `pyproject.toml`.

    If you don't have `uv` installed, follow the instructions on the [uv website](https://github.com/astral-sh/uv#installation) to install it first.

## Configuration

Configuration is managed via environment variables, which can also be loaded from a `.env` file in the project root.

**Required Environment Variables**:

-   `MQ_ADAPTER`: Specifies the message queue adapter to use.
    -   Example: `MQ_ADAPTER="rabbitmq"`
-   `MQ_URL`: The connection URL for the message queue broker.
    -   Example for RabbitMQ: `MQ_URL="amqp://guest:guest@localhost:5672/"`

**Optional Environment Variables**:

-   `MQ_DEFAULT_TOPIC`: A default topic name used by examples or if no topic is specified.
    -   Example: `MQ_DEFAULT_TOPIC="my_default_topic"`

Create a `.env` file in the project root to store these variables for local development:
```ini
# .env
MQ_ADAPTER="rabbitmq"
MQ_URL="amqp://guest:guest@localhost:5672/"
MQ_DEFAULT_TOPIC="local_dev_topic"
```
An example `.env` file is provided as `.env.example`. Rename it to `.env` and customize it.

## Usage

The core components are the `Message` class (from `message_service.model.message`), producer/consumer factories (`create_producer`, `create_consumer` from `message_service`), and the abstract interfaces they return.

### Basic Example

Here's a simplified example of sending and receiving a message:

```python
import asyncio
from message_service import create_producer, create_consumer, Message, settings

async def handle_message(message: Message):
    print(f"Received: {message.body}")
    # Further processing...

async def main():
    # Ensure MQ_ADAPTER and MQ_URL are set in your environment or .env file
    print(f"Using adapter: {settings.mq_adapter} with URL: {settings.mq_url}")

    producer = create_producer()
    consumer = create_consumer()

    await producer.connect()
    await consumer.connect()

    # Consumer subscribes to a topic
    # For RabbitMQ, this involves an exchange and a routing key (topic)
    topic = "my_app_topic"
    await consumer.subscribe(
        topic=topic,
        callback=handle_message,
        exchange_name="app_exchange", # Default exchange type is 'direct'
        queue_name=f"{topic}_q"
    )

    # Start consuming in the background
    consumer_task = asyncio.create_task(consumer.start_consuming())
    print(f"Consumer subscribed to '{topic}' and started.")

    # Producer sends a message
    my_message = Message(body="Hello Unified MQ!")
    await producer.publish_message(
        message=my_message,
        topic=topic,
        exchange_name="app_exchange" # Default exchange type is 'direct'
    )
    print(f"Message '{my_message.id}' sent to topic '{topic}'.")

    await asyncio.sleep(2) # Keep alive for a bit

    # Cleanup
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            print("Consumer task cancelled.")
    await consumer.disconnect()
    await producer.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

For a more detailed, runnable example demonstrating direct, delayed, and broadcast (fanout) messaging, see `examples/simple_usage.py`.
Refer to `examples/README.md` for instructions on running it.

## Running Examples

1.  Ensure your `PYTHONPATH` includes the project root (which contains the `message-service` package directory):
    ```bash
    export PYTHONPATH=$(pwd):$PYTHONPATH
    ```
    (Adjust for your shell if not bash/zsh. This allows `import message_service`)
2.  Make sure your chosen message queue broker (e.g., RabbitMQ) is running and configured in your `.env` file or environment variables. For delayed messages with RabbitMQ, ensure the `rabbitmq-delayed-message-exchange` plugin is enabled on the server.
3.  Navigate to the `examples` directory and run the desired script:
    ```bash
    python examples/simple_usage.py
    ```

## Running Tests

1.  Ensure your `PYTHONPATH` includes the project root (see above).
2.  Run the test script from the project root:
    ```bash
    ./run_tests.sh
    ```
    This script likely runs both `unittest` and `pytest` tests.
    Alternatively, use Python's `unittest` or `pytest` directly:
    ```bash
    # For unittest-based tests
    python -m unittest discover -v tests
    # For pytest-based tests (like the new adapter tests)
    pytest
    ```

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
(Further details on contribution guidelines can be added here).

## License

This project is licensed under the MIT License. (Ensure a LICENSE file exists in the repository)
