import argparse
import csv
import json
import logging
import os
import time
from pathlib import Path

from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def publish_csv(
    csv_path: Path,
    project_id: str,
    topic_id: str,
    interval_seconds: float,
) -> int:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    published_count = 0

    with csv_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames:
            raise ValueError("CSV file must include a header row.")

        for line_number, row in enumerate(reader, start=2):
            message_data = json.dumps(row, allow_nan=False).encode("utf-8")
            message_id = publisher.publish(
                topic_path,
                message_data,
                csv_row=str(line_number),
                message_type="csv_record",
            ).result()
            published_count += 1
            logger.info("Published CSV line %d as message %s", line_number, message_id)
            if interval_seconds:
                time.sleep(interval_seconds)

    return published_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish weather CSV rows to Pub/Sub.")
    parser.add_argument("csv_file", type=Path, help="Path to the input CSV file.")
    parser.add_argument(
        "--project",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="Google Cloud project ID (or set GCP_PROJECT_ID).",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("PUBSUB_TOPIC_ID", "weather"),
        help="Pub/Sub topic ID (or set PUBSUB_TOPIC_ID).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="Delay between published messages in seconds.",
    )
    args = parser.parse_args()

    if not args.project:
        parser.error("Set GCP_PROJECT_ID or pass --project.")
    if args.interval < 0:
        parser.error("--interval cannot be negative.")

    count = publish_csv(args.csv_file, args.project, args.topic, args.interval)
    logger.info("Published %d record(s).", count)


if __name__ == "__main__":
    main()
