from abc import ABC, abstractmethod
from typing import Any
from ..unified_message_model import Message

class AbstractProducer(ABC):
    @abstractmethod
    async def connect(self, **kwargs: Any) -> None:
        """Connects to the message queue broker."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnects from the message queue broker."""
        pass

    @abstractmethod
    async def publish_message(self, message: Message, topic: str, **kwargs: Any) -> None:
        """
        Publishes a message to a specified topic or queue.

        Args:
            message: The Message object to publish.
            topic: The topic or queue name to publish to.
            **kwargs: Additional parameters specific to the middleware.
        """
        pass
