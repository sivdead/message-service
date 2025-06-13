from abc import ABC, abstractmethod
from typing import Any
from ..unified_message_model import Message

class AbstractProducer(ABC):
    @abstractmethod
    async def connect(self, **kwargs: Any) -> None:
        """
        Establishes a connection to the message queue broker.
        
        Additional connection parameters can be provided as keyword arguments, allowing for middleware-specific configuration.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Closes the connection to the message queue broker.
        """
        pass

    @abstractmethod
    async def publish_message(self, message: Message, topic: str, **kwargs: Any) -> None:
        """
        Publishes a message to a specified topic or queue.
        
        Args:
            message: The message to be published.
            topic: The target topic or queue name.
            **kwargs: Additional middleware-specific parameters.
        """
        pass
