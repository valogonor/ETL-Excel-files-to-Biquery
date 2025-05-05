# Code to be entered into Cloud Run function
from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
import pandas as pd
import io, os

app = Flask(__name__)

@app.route("/", methods=["POST"])
def clean_and_upload_to_bigquery(request):
    data = request.get_json()
    bucket_name = data["bucket"]
    files = data["files"]  # list of filenames

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    for file_name in files:
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_bytes()
        df = pd.read_excel(io.BytesIO(content))

        # Drop "Unnamed" columns
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

        # Extract just the file name (e.g., "data1.xlsx")
        base_name = os.path.basename(file_name)

        # Create a sanitized table name
        table_id = f"csv-to-bigquery-demo-457718.tech_jobs.{base_name.replace('.xlsx', '')}"


        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Loaded {file_name} into {table_id}")

    return jsonify({"status": "success"}), 200
