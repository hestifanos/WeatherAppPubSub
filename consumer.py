import argparse
import csv
import json
import logging
import os
import threading
from concurrent.futures import CancelledError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MessageProcessor:
    def __init__(self, csv_path: Path | None = None) -> None:
        self.csv_path = csv_path
        self._csv_file = None
        self._csv_writer = None
        self._lock = threading.Lock()
        self.message_count = 0
        self.failure_count = 0

    def process(self, message: pubsub_v1.subscriber.message.Message) -> None:
        try:
            record: Any = json.loads(message.data.decode("utf-8"))
            if not isinstance(record, dict):
                raise ValueError("Expected a JSON object.")

            processed_record = dict(record)
            processed_record["_processing_metadata"] = {
                "consumed_at": datetime.now(timezone.utc).isoformat(),
                "message_id": message.message_id,
            }
            if message.publish_time:
                latency = datetime.now(timezone.utc) - message.publish_time
                processed_record["_processing_metadata"]["latency_seconds"] = (
                    latency.total_seconds()
                )

            with self._lock:
                if self.csv_path:
                    self._write_csv(processed_record)
                self.message_count += 1
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, OSError):
            with self._lock:
                self.failure_count += 1
            logger.exception("Could not process Pub/Sub message.")
            message.nack()
            return

        logger.info("Consumed record: %s", processed_record)
        message.ack()

    def _write_csv(self, record: dict[str, Any]) -> None:
        if self.csv_path is None:
            return
        if self._csv_file is None:
            self._csv_file = self.csv_path.open("w", newline="", encoding="utf-8")
            self._csv_writer = csv.DictWriter(
                self._csv_file,
                fieldnames=list(record),
                extrasaction="ignore",
            )
            self._csv_writer.writeheader()

        if self._csv_writer is None or self._csv_file is None:
            raise RuntimeError("CSV output writer was not initialized.")

        row = {
            key: json.dumps(value, ensure_ascii=False)
            if isinstance(value, (dict, list))
            else value
            for key, value in record.items()
        }
        self._csv_writer.writerow(row)
        self._csv_file.flush()

    def close(self) -> None:
        if self._csv_file is not None:
            self._csv_file.close()
            self._csv_file = None
            self._csv_writer = None


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume weather Pub/Sub messages.")
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="Google Cloud project ID (or set GCP_PROJECT_ID).",
    )
    parser.add_argument(
        "--subscription",
        default=os.environ.get("PUBSUB_SUBSCRIPTION_ID", "weather-sub"),
        help="Subscription ID (or set PUBSUB_SUBSCRIPTION_ID).",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Optional path for writing consumed records to CSV.",
    )
    args = parser.parse_args()

    if not args.project:
        parser.error("Set GCP_PROJECT_ID or pass --project.")

    processor = MessageProcessor(args.csv)
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(args.project, args.subscription)
    logger.info("Listening for messages on %s", subscription_path)

    try:
        with subscriber:
            streaming_pull = subscriber.subscribe(
                subscription_path,
                callback=processor.process,
            )
            try:
                streaming_pull.result()
            except KeyboardInterrupt:
                streaming_pull.cancel()
                try:
                    streaming_pull.result()
                except CancelledError:
                    logger.info("Subscriber stopped.")
    finally:
        processor.close()
        logger.info(
            "Processed %d message(s); %d failure(s).",
            processor.message_count,
            processor.failure_count,
        )


if __name__ == "__main__":
    main()
