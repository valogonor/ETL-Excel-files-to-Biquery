from google.cloud import storage
import requests

# Initialize Cloud Storage client (make sure you're authenticated)
storage_client = storage.Client()

# Configuration
BUCKET_NAME = "csv-to-bigquery-demo"
FOLDER_PREFIX = "tech_jobs/"  # Include trailing slash if needed
CLOUD_RUN_URL = "https://clean-and-upload-to-bigquery-527952229907.us-central1.run.app"  # Replace with your endpoint

# List files in the bucket under the given folder
bucket = storage_client.bucket(BUCKET_NAME)
blobs = bucket.list_blobs(prefix=FOLDER_PREFIX)

# Filter for Excel files
excel_files = [blob.name for blob in blobs if blob.name.endswith(".xlsx")]

# Optional: remove the folder prefix if needed (depends on how you handle it in your ETL)
# excel_files = [name.split(FOLDER_PREFIX)[-1] for name in excel_files]

# Send to Cloud Run
payload = {
    "bucket": BUCKET_NAME,
    "files": excel_files
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(CLOUD_RUN_URL, json=payload, headers=headers)

if response.status_code == 200:
    print("✅ ETL job triggered successfully!")
    print(response.json())
else:
    print("❌ Failed to trigger ETL job.")
    print("Status code:", response.status_code)
    print("Response:", response.text)
