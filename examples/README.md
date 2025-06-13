# Examples

This directory contains example scripts demonstrating how to use the unified messaging library.

## Running Examples

1.  **Ensure `src` is in your `PYTHONPATH`**:
    From the project root directory, run:
    ```bash
    export PYTHONPATH=$(pwd):$PYTHONPATH
    ```
    Alternatively, you can install the project in editable mode if `pyproject.toml` is set up for a package.

2.  **Set up Environment Variables (or `.env` file)**:
    The examples use configuration loaded via `src.config`. You can define environment variables like `MQ_ADAPTER` and `MQ_URL`, or create a `.env` file in the project root.

    Example `.env` for RabbitMQ:
    ```
    MQ_ADAPTER="rabbitmq"
    MQ_URL="amqp://guest:guest@localhost:5672/"
    MQ_DEFAULT_TOPIC="example_topic"
    ```

3.  **Ensure Message Queue Broker is Running**:
    For instance, if using the RabbitMQ adapter, make sure a RabbitMQ server is running and accessible via the configured `MQ_URL`.

4.  **Run the script**:
    ```bash
    python examples/simple_usage.py
    ```
