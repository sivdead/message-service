import unittest
import uuid
from datetime import datetime, timezone
from src.unified_message_model import Message

class TestMessageModel(unittest.TestCase):

    def test_message_creation_defaults(self):
        body = "Test message body"
        msg = Message(body=body)
        self.assertEqual(msg.body, body)
        self.assertIsNotNone(msg.id)
        try:
            uuid.UUID(msg.id, version=4)
        except ValueError:
            self.fail("Default message ID is not a valid UUIDv4")
        self.assertIsInstance(msg.timestamp, datetime)
        self.assertLessEqual(datetime.now(timezone.utc) - msg.timestamp,                              datetime.now(timezone.utc) - msg.timestamp) # Check it's recent
        self.assertEqual(msg.headers, {})

    def test_message_creation_custom_values(self):
        body = b"Binary body"
        msg_id = "custom-id-123"
        headers = {"key1": "value1", "key2": 2}
        now = datetime.now(timezone.utc)
        msg = Message(body=body, message_id=msg_id, headers=headers, timestamp=now)

        self.assertEqual(msg.body, body)
        self.assertEqual(msg.id, msg_id)
        self.assertEqual(msg.headers, headers)
        self.assertEqual(msg.timestamp, now)

    def test_message_representation(self):
        msg = Message(body="Hello", message_id="test-repr-id")
        representation = repr(msg)
        self.assertIn("Message(id='test-repr-id'", representation)
        self.assertIn("body='Hello'", representation)
        self.assertIn("headers={}", representation)
        self.assertIn("timestamp=", representation)

if __name__ == '__main__':
    unittest.main()
