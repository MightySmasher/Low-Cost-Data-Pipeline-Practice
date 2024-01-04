from google.cloud import bigquery
import os

# Input your Config in runtime environment setting when creating cloud function
class Config:
    bucket_name = os.environ.get('bucket_name')
    destination_blob_name = os.environ.get('destination_blob_name')
    project_id = os.environ.get("project_id")
    dataset_name = os.environ.get("dataset_name")
    table_name = os.environ.get("table_name")


def batch_upload():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Get destination table id
    table_id = f'{Config.project_id}.{Config.dataset_name}.{Config.table_name}'

    # Config job for uploading 
    job_config = bigquery.LoadJobConfig(
        autodetect=True,    # autodetect schema
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # use overwrite method
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,field="timestamp") # partitioning
    )
    blob_uri = f"gs://{Config.bucket_name}/{Config.destination_blob_name}" # source file url
    load_job = client.load_table_from_uri(blob_uri, table_id, job_config=job_config) # Make an API request.
    load_job.result()  # Wait for the job to complete.

def gcs_to_bq(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    batch_upload()