import datetime
import json
import os
import subprocess
import threading

import google.auth.credentials
from google.api_core.exceptions import NotFound
from google.auth import _helpers, impersonated_credentials
from google.cloud import pubsub_v1, storage

PROJECT_ID = "solar-attic-508721-n0"
SUBSCRIPTION_ID = "forbidden-requests-sub"
BUCKET_NAME = "cs528hww2"
LOG_FILE = "forbidden-logs/forbidden_requests.log"  # separate directory in the bucket
SERVICE_ACCOUNT = "hw3-474@solar-attic-508721-n0.iam.gserviceaccount.com"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


class GcloudUserCredentials(google.auth.credentials.Credentials):
    """Your own `gcloud auth login` identity.
    Used ONLY to ask Google for short-lived tokens for the service account."""

    def refresh(self, request):
        self.token = subprocess.check_output(
            ["gcloud", "auth", "print-access-token"],
            text=True, shell=(os.name == "nt"),
        ).strip()
        self.expiry = _helpers.utcnow() + datetime.timedelta(minutes=50)


# Impersonate the same service account Service 1 uses (no key file needed)
creds = impersonated_credentials.Credentials(
    source_credentials=GcloudUserCredentials(),
    target_principal=SERVICE_ACCOUNT,
    target_scopes=SCOPES,
    lifetime=3600,
)

storage_client = storage.Client(project=PROJECT_ID, credentials=creds)
log_blob = storage_client.bucket(BUCKET_NAME).blob(LOG_FILE)
subscriber = pubsub_v1.SubscriberClient(credentials=creds)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

write_lock = threading.Lock()  # messages can arrive at the same time


def append_to_bucket(line):
    """Cloud Storage files can't be appended to directly: read, add the line, write back."""
    with write_lock:
        try:
            existing = log_blob.download_as_text()
        except NotFound:
            existing = ""
        log_blob.upload_from_string(existing + line + "\n", content_type="text/plain")


def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        line = (f"{now} FORBIDDEN: {data.get('method')} request from "
                f"{data.get('country')} for {data.get('filename')} was denied")
        print(line, flush=True)
        append_to_bucket(line)
        message.ack()
    except Exception as e:
        print(f"Error handling message: {e}", flush=True)
        message.nack()


def main():
    print(f"Listening on {subscription_path} as {SERVICE_ACCOUNT} (Ctrl+C to stop)")
    future = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()
            future.result()


if __name__ == "__main__":
    main()
