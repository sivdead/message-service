# This file makes 'src' a package, facilitating imports from its submodules
from .unified_message_model import Message
from .mq_abstraction_layer import AbstractProducer, AbstractConsumer
from .config import settings
from .mq_factory import create_producer, create_consumer, UnsupportedMQAdapterError

__all__ = [
    'Message',
    'AbstractProducer',
    'AbstractConsumer',
    'settings',
    'create_producer',
    'create_consumer',
    'UnsupportedMQAdapterError',
]
