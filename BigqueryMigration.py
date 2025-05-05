import os
import pandas as pd
import time
from google.cloud import bigquery, storage
from pathlib import Path

def table_reference(project_id, dataset_id, table_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_id)
    return table_ref

def delete_dataset_tables(project_id, dataset_id):
    tables = client.list_tables(f'{project_id}.{dataset_id}')
    for table in tables:
        client.delete_table(table)
    print('Tables deleted.')

def convert_xlsx_to_csv(xlsx_path):
    df = pd.read_excel(xlsx_path)
    csv_path = xlsx_path.with_suffix('.csv')
    df.to_csv(csv_path, index=False)
    return csv_path

def upload_csv(client, table_ref, csv_file):
    client.delete_table(table_ref, not_found_ok=True)

    load_job_configuration = bigquery.LoadJobConfig()
    # load_job_configuration.schema = [
    #     bigquery.SchemaField('<field name1>', '<data type>', mode='<mode type>'),
    #     bigquery.SchemaField('<field name2>', '<data type>', mode='<mode type>'),
    #     bigquery.SchemaField('<field name3>', '<data type>', mode='<mode type>')
    # ]

    load_job_configuration.autodetect = True
    load_job_configuration.source_format = bigquery.SourceFormat.CSV
    load_job_configuration.skip_leading_rows = 1
    load_job_configuration.allow_quoted_newlines = True
    # load_job_configuration.column_name_character_map='V2'

    with open(csv_file, 'rb') as source_file:
        upload_job = client.load_table_from_file(
            source_file,
            destination=table_ref,          
            location='US', # Must match the destination dataset location
            job_config=load_job_configuration,
        )

    while upload_job.state != 'DONE':
        time.sleep(2)
        upload_job.reload()
        print(upload_job.state)
    print(upload_job.result())

def upload_df(client, table_ref, df):
    client.delete_table(table_ref, not_found_ok=True)

    load_job_configuration = bigquery.LoadJobConfig()
    # load_job_configuration.schema = [
    #     bigquery.SchemaField('<field name1>', '<data type>', mode='<mode type>'),
    #     bigquery.SchemaField('<field name2>', '<data type>', mode='<mode type>'),
    #     bigquery.SchemaField('<field name3>', '<data type>', mode='<mode type>')
    # ]

    load_job_configuration.autodetect = True
    load_job_configuration.source_format = bigquery.SourceFormat.CSV
    load_job_configuration.skip_leading_rows = 1
    load_job_configuration.allow_quoted_newlines = True

    upload_job = client.load_table_from_dataframe(df, table_ref, job_config=load_job_configuration)

    while upload_job.state != 'DONE':
        time.sleep(2)
        upload_job.reload()
        print(upload_job.state)
    print(upload_job.result())

def upload_excel_files(folder_path, bucket_name, destination_folder):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            print(f'Processing {filename}')
            blob = bucket.blob(f"{destination_folder}/{filename}")
            blob.upload_from_filename(os.path.join(folder_path, filename), timeout=600)
            print(f"Uploaded {filename} to {destination_folder}/")

# Prepare the variables
project_id = "csv-to-bigquery-demo-457718"
dataset_id = "tech_jobs"

client = bigquery.Client(project=project_id)
data_file_folder = Path("./Tech Jobs Dataset")
directory_list = os.listdir(data_file_folder)

# To load data from Excel files to GCP Cloud Storage
upload_excel_files(
    folder_path=data_file_folder,
    bucket_name="csv-to-bigquery-demo",
    destination_folder="tech_jobs"
)

# # To load data straight from Excel files to Bigquery
# for file in directory_list:
#     if file.endswith('xlsx'):
#         print("Processing file: {}".format(file))
#         table_name = file[:-5]
#         df = pd.read_excel(data_file_folder / file)
#         todrop = []
#         for column in df.columns:
#             if column.startswith('Unnamed'):
#                 todrop.append(column)
#         df = df.drop(columns=todrop)
#         xlsx_file = data_file_folder / file
#         if table_name + '.csv' in directory_list:
#             csv_file = data_file_folder / (table_name + '.csv')
#         else:
#             csv_file = convert_xlsx_to_csv(xlsx_file)
#         table_ref = table_reference(project_id, dataset_id, table_name)
#         upload_df(client, table_ref, df)
#         print()
