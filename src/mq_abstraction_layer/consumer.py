from abc import ABC, abstractmethod
from typing import Any, Callable
from ..unified_message_model import Message

class AbstractConsumer(ABC):
    @abstractmethod
    async def connect(self, **kwargs: Any) -> None:
        """Connects to the message queue broker."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnects from the message queue broker."""
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[Message], Any], **kwargs: Any) -> None:
        """
        Subscribes to a topic or queue and registers a callback for incoming messages.

        Args:
            topic: The topic or queue name to subscribe to.
            callback: A callable that will be invoked with the received Message.
                      The callback can be a regular function or an async function.
                      The consumer implementation should handle calling it appropriately.
            **kwargs: Additional parameters specific to the middleware.
        """
        pass

    @abstractmethod
    async def start_consuming(self) -> None:
        """Starts the message consumption process."""
        pass

    @abstractmethod
    async def stop_consuming(self) -> None:
        """Stops the message consumption process."""
        pass
