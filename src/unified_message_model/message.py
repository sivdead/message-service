import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Union

class Message:
    def __init__(
        self,
        body: Union[str, bytes],
        headers: Dict[str, Any] = None,
        message_id: str = None,
        timestamp: datetime = None,
    ):
        self.id: str = message_id if message_id is not None else str(uuid.uuid4())
        self.body: Union[str, bytes] = body
        self.headers: Dict[str, Any] = headers if headers is not None else {}
        self.timestamp: datetime = timestamp if timestamp is not None else datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return (
            f"Message(id='{self.id}', body='{self.body!r}', "
            f"headers={self.headers}, timestamp={self.timestamp.isoformat()})"
        )

    # Future methods for serialization/deserialization can be added here
    # For example, to_json, from_json, to_dict, from_dict

if __name__ == '__main__':
    # Example Usage
    msg1 = Message(body="Hello, world!")
    print(f"Message 1: {msg1}")

    msg2 = Message(
        body=b'Some binary data',
        headers={'content_type': 'application/octet-stream', 'priority': 'high'},
        message_id='custom-id-123'
    )
    print(f"Message 2: {msg2}")
