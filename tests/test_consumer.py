import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from consumer import MessageProcessor


class FakeMessage:
    message_id = "message-1"
    publish_time = None

    def __init__(self, data):
        self.data = data
        self.acknowledged = False
        self.rejected = False

    def ack(self):
        self.acknowledged = True

    def nack(self):
        self.rejected = True


class MessageProcessorTests(unittest.TestCase):
    def test_valid_record_is_acknowledged_and_written_to_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "records.csv"
            processor = MessageProcessor(path)
            message = FakeMessage(b'{"temperature": 21}')

            processor.process(message)
            processor.close()

            self.assertTrue(message.acknowledged)
            self.assertEqual(processor.message_count, 1)
            with path.open(newline="", encoding="utf-8") as csv_file:
                row = next(csv.DictReader(csv_file))
            self.assertEqual(row["temperature"], "21")
            self.assertEqual(
                json.loads(row["_processing_metadata"])["message_id"],
                "message-1",
            )

    def test_invalid_json_is_rejected(self):
        processor = MessageProcessor()
        message = FakeMessage(b"not-json")

        with patch("consumer.logger.exception"):
            processor.process(message)

        self.assertTrue(message.rejected)
        self.assertEqual(processor.failure_count, 1)


if __name__ == "__main__":
    unittest.main()
