# Unified Message Queue Library

A Python library for interacting with various message queue middlewares through a unified interface. This library allows developers to write message publishing and consuming logic once and easily switch between different message queue systems by changing configuration.

## Features

- **Unified Message Model**: A consistent `Message` object across different MQ systems.
- **Abstraction Layer**: `AbstractProducer` and `AbstractConsumer` interfaces for common MQ operations.
- **Pluggable Adapters**: Easily extendable with new adapters for different MQ middlewares.
- **Configuration-Driven**: Switch MQ backends by changing environment variables or a `.env` file.
- **Asynchronous**: Built with `asyncio` for modern asynchronous Python applications.

## Current Supported Middlewares

- **RabbitMQ** (via `aio-pika`)

## Project Structure

```
.
├── examples/                # Example usage scripts
│   ├── simple_usage.py
│   └── README.md
├── src/                     # Source code
│   ├── adapters/            # Concrete MQ adapter implementations
│   │   └── rabbitmq/        # RabbitMQ specific adapter
│   │       ├── __init__.py
│   │       ├── consumer.py
│   │       └── producer.py
│   ├── mq_abstraction_layer/ # Abstract producer/consumer interfaces
│   │   ├── __init__.py
│   │   ├── consumer.py
│   │   └── producer.py
│   ├── unified_message_model/ # The generic Message class
│   │   ├── __init__.py
│   │   └── message.py
│   ├── __init__.py          # Makes 'src' a package
│   ├── config.py            # Configuration loading (env vars, .env)
│   └── mq_factory.py        # Factory for creating producer/consumer instances
├── tests/                   # Unit tests
│   ├── adapters/
│   │   └── test_rabbitmq_adapter.py
│   ├── __init__.py
│   ├── test_config.py
│   ├── test_message_model.py
│   └── test_mq_factory.py
├── .env.example             # Example .env file (renamed from .env for commit)
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

The core components are the `Message` class, producer/consumer factories (`create_producer`, `create_consumer`), and the abstract interfaces they return.

### Basic Example

Here's a simplified example of sending and receiving a message:

```python
import asyncio
from src import create_producer, create_consumer, Message, settings

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
        # RabbitMQ-specific params (can be omitted if defaults in adapter are fine)
        exchange_name="app_exchange",
        queue_name=f"{topic}_q"
    )

    # Start consuming in the background (actual implementation might vary)
    # For RabbitMQ, the current consumer's start_consuming() might block
    # or needs to be run as a task.
    consumer_task = asyncio.create_task(consumer.start_consuming())
    print(f"Consumer subscribed to '{topic}' and started.")

    # Producer sends a message
    my_message = Message(body="Hello Unified MQ!")
    await producer.publish_message(
        message=my_message,
        topic=topic,
        # RabbitMQ-specific params
        exchange_name="app_exchange"
    )
    print(f"Message '{my_message.id}' sent to topic '{topic}'.")

    # Keep alive for a bit to receive message or use an event for synchronization
    await asyncio.sleep(2)

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

For a more detailed, runnable example, see `examples/simple_usage.py`.
Refer to `examples/README.md` for instructions on running it.

## Running Examples

1.  Ensure your `PYTHONPATH` includes the project root:
    ```bash
    export PYTHONPATH=$(pwd):$PYTHONPATH
    ```
    (Adjust for your shell if not bash/zsh)
2.  Make sure your chosen message queue broker (e.g., RabbitMQ) is running and configured in your `.env` file or environment variables.
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
    Alternatively, use Python's `unittest` module directly:
    ```bash
    python -m unittest discover -v tests
    ```

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
(Further details on contribution guidelines can be added here).

## License

This project is licensed under the MIT License. (Add a LICENSE file if desired)
