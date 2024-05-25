import json
import uuid
from google.cloud import pubsub_v1
from google.cloud import storage
from google.oauth2 import service_account

# Configuration details
PROJECT_ID = 'de-class-activity-420602'
SUBSCRIPTION_ID = 'archive-sub'
SERVICE_ACCOUNT_KEY = '/home/lukulapu/data.json'
GCS_BUCKET = 'new_bucket'

# Initialize credentials
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_KEY,
    scopes=["https://www.googleapis.com/auth/pubsub", "https://www.googleapis.com/auth/cloud-platform"]
)

# Initialize Pub/Sub subscriber client
subscriber_client = pubsub_v1.SubscriberClient(credentials=creds)
subscription_path = subscriber_client.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# Initialize GCS client
gcs_client = storage.Client(credentials=creds)
gcs_bucket = gcs_client.bucket(GCS_BUCKET)


def message_handler(message):
    """Handles incoming Pub/Sub messages and stores them in Google Cloud Storage."""
    try:
        # Decode and parse message data
        payload = json.loads(message.data.decode('utf-8'))
        unique_id = str(uuid.uuid4())
        blob_name = f"message_{unique_id}.json"
        blob = gcs_bucket.blob(blob_name)

        # Upload the message data to GCS
        blob.upload_from_string(json.dumps(payload))
        print(f"Successfully stored data in GCS: {blob_name}")

        # Acknowledge the message
        message.ack()
    except Exception as err:
        print(f"Error processing message: {err}")
        message.nack()  # Not acknowledge to retry the message


def start_subscription():
    """Start the subscription to listen for messages."""
    streaming_future = subscriber_client.subscribe(subscription_path, callback=message_handler)
    print(f"Listening for messages on {subscription_path}...")

    with subscriber_client:
        try:
            streaming_future.result()  # Keep the main thread active
        except Exception as err:
            streaming_future.cancel()
            streaming_future.result()  # Wait until the shutdown is complete
            raise err


if __name__ == "__main__":
    start_subscription()
