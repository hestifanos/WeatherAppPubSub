# Weather Data Pub/Sub

A Python example for publishing weather records from a CSV file to Google Cloud Pub/Sub and consuming messages from a subscription. The consumer can also collect processed messages and basic run statistics.

## Requirements

- Python 3.10 or newer
- A Google Cloud project with a `weather` topic and `weather-sub` subscription
- A service account or Application Default Credentials with least-privilege Pub/Sub access

Install the client library:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install google-cloud-pubsub
```

Set `GCP_PROJECT_ID` in the environment, and authenticate with `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS` to a protected service-account file; never commit credential files.

Run `producer.py` with the source CSV path and start `consumer.py` to listen for messages. Create the topic and subscription in Google Cloud before running the examples.

```bash
export GCP_PROJECT_ID="your-development-project"
python producer.py path/to/weather.csv --topic weather
python consumer.py --subscription weather-sub --csv consumed_messages.csv
```

## Components

- `producer.py`: reads CSV rows and publishes serialized records.
- `consumer.py`: subscribes to messages and processes received records.


