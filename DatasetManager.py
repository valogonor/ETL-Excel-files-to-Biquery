from google.cloud import bigquery, bigquery_datatransfer

class DatasetManager:
    def __init__(self, client):
        self.client = client

    def delete_dataset(self, dataset_id):
        # update `not_found_ok` parameter to ignore raise exception if dataset is absent.
        self.client.delete_dataset(dataset_id, not_found_ok=True)

    def create_dataset(self, dataset_id, location='US'):
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset_ = self.client.create_dataset(dataset, timeout=30)
        print("Dataset {}.{} created".format(self.client.project, dataset.dataset_id))
        return dataset_

    def list_dataset(self):
        datasets = self.client.list_datasets()
        return [dataset.dataset_id for dataset in datasets]

    def copy_dataset(self, source_project_id, source_dataset_id, destination_project_id, destination_dataset_id, display_name):
        """
        Reference:
        https://cloud.google.com/bigquery-transfer/docs/reference/datatransfer/rest/v1/projects.transferConfigs

        Grant IAM Access:
        https://cloud.google.com/bigquery/docs/enable-transfer-service#grant_bigqueryadmin_access

        Note:
        - this will move the tables also.
        - enable BigQuery Data Transfer API page in the API library.
        """
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()
        transfer_config = bigquery_datatransfer.TransferConfig(
            destination_dataset_id=destination_dataset_id,
            display_name=display_name,
            data_source_id="cross_region_copy",
            params={
                "source_project_id": source_project_id,
                "source_dataset_id": source_dataset_id,
            },
            # More schedule foramt example:
            # https://cloud.google.com/appengine/docs/flexible/python/scheduling-jobs-with-cron-yaml#the_schedule_format
            # schedule="every 24 hours",
        )
        transfer_config = transfer_client.create_transfer_config(
            parent=transfer_client.common_project_path(destination_project_id),
            transfer_config=transfer_config,
        )
        print(f"Created transfer config: {transfer_config.name}")

# Construct BigQuery Client Object
client = bigquery.Client(project="csv-to-bigquery-demo-457718")
dataset_manager = DatasetManager(client)

# # Example 1. List Datasets
# dataset_list = dataset_manager.list_dataset()
# print(dataset_list)

# Example 2. Create A Dataset
dataset_name = f'{client.project}.tech_jobs'
dataset = dataset_manager.create_dataset(dataset_name)


# # Example 3. Delete A Dataset
# dataset_name = f'{client.project}.trash1'
# dataset_manager.delete_dataset(dataset_name)

# # Example 4. Move Tables From A Dataset
# # source_project_id = 'sql-for-bigquery'
# # source_dataset_id = 'Demo'
# # dest_project_id = 'sql-for-bigquery'
# # dest_dataset_id = 'trash1'
# # response = dataset_manager.copy_dataset(source_project_id, source_dataset_id, dest_project_id, dest_dataset_id, 'datset transfer demo1')

# source_project_id = 'sql-for-bigquery'
# source_dataset_id = 'JJ'
# dest_project_id = 'noble-airport-243704'
# dest_dataset_id = 'cloud_demo'
# response = dataset_manager.copy_dataset(source_project_id, source_dataset_id, dest_project_id, dest_dataset_id, 'datset transfer demo2')
# print(response)

# Class and example code courtesy of Jie Jenn.