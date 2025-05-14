# ETL Excel Files to BigQuery

This project provides a streamlined ETL (Extract, Transform, Load) pipeline to automate the process of importing Excel files into Google BigQuery. It specifically uses a Tech Jobs Dataset obtained from Kaggle here: https://www.kaggle.com/datasets/christopherkverne/100k-us-tech-jobs-winter-2024.

## Features

- **Automated Extraction**: Reads data from Excel files (`.xlsx`) using Python.
- **Data Transformation**: Cleans and transforms data to match BigQuery schema requirements.
- **Cloud Integration**: Uploads transformed data to Google Cloud Storage (GCS).
- **BigQuery Loading**: Loads data from GCS into BigQuery tables.
- **Configurable Parameters**: Allows customization of source files, GCS buckets, and BigQuery datasets/tables.

## Prerequisites

Before setting up the project, ensure you have the following:

- **Python 3.7 or higher**: [Download Python](https://www.python.org/downloads/)
- **Google Cloud SDK**: [Install Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- **Google Cloud Project**: With BigQuery and Cloud Storage APIs enabled.
- **Service Account**: With appropriate permissions and a JSON key file.

## Files

### DatasetManager.py

DatasetManager.py can be used to create the dataset that will hold the tables that the data from the Excel files will be entered into. The file includes commented out examples of other ways to use the file, including listing datasets, deleting a dataset, and moving tables from a dataset. Constructing the Bigquery client object requires that the project ID of the project in which the dataset is to be created get entered as an attribute to the client object as shown.

### BigqueryMigration.py

BigqueryMigration.py is used to upload Excel files stored locally into Google Cloud Storage (GCS). The bottom of the file contains a commented out section that can be uncommented and used to load data straight from the Excel files into Bigquery in cases in which storage to GCS in unnecessary.

### main.py and requirements.txt

These two files are included as source files in a function created with Google Cloud's Cloud Run platform. The Cloud Run function extracts the data from the Excel files, transforms it (in this case dropping columns starting with "Unnamed"), then loads the transformed data into Bigquery. After the Cloud Run function is created and deployed, the function will be assigned a service URL which is used as an HTTP endpoint that is used to initialize the CLOUD_RUN_URL variable in the trigger_etl.py file.

### trigger_etl.py

After the Excel files have been loaded to GCS and the Cloud Run function coding the ETL job has been created and deployed, the trigger_etl.py file can be run to trigger the ETL job. The variables that need to be configured before running the file are BUCKET_NAME, FOLDER_PREFIX, and CLOUD_RUN_URL. BUCKET_NAME should be set to the GCS bucket that the Excel files are stored in. FOLDER_PREFIX should be set to the folder within the bucket that contains the Excel files (can be an empty string if the files are not inside a folder in the bucket). CLOUD_RUN_URL should be set to the URL provided after deploying the Cloud Run function that executes the ETL job.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.