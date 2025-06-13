from abc import ABC, abstractmethod
from typing import Any, Callable
from ..unified_message_model import Message

class AbstractConsumer(ABC):
    @abstractmethod
    async def connect(self, **kwargs: Any) -> None:
        """
        Establishes an asynchronous connection to the message queue broker.
        
        Additional connection parameters can be provided as keyword arguments, allowing support for various broker-specific options.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnects from the message queue broker.
        
        This method should be implemented to close any active connections and release resources associated with the consumer.
        """
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[Message], Any], **kwargs: Any) -> None:
        """
        Subscribes to a topic or queue and registers a callback for each received message.
        
        The callback is invoked with every incoming Message object. Supports both synchronous and asynchronous callbacks. Additional middleware-specific options can be provided via keyword arguments.
        """
        pass

    @abstractmethod
    async def start_consuming(self) -> None:
        """
        Begins consuming messages from the message queue.
        
        This method should be implemented to start the message processing loop, enabling the consumer to receive and handle messages as they arrive.
        """
        pass

    @abstractmethod
    async def stop_consuming(self) -> None:
        """
        Stops the message consumption process.
        
        This method should be implemented to halt the ongoing message consumption loop in the consumer.
        """
        pass
