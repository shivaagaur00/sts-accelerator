"""

gcs_manager.py
 
Verifies that a table's Parquet files actually landed in GCS after

the STS transfer, and applies the lifecycle cleanup once BigQuery

load + validation succeed.
 
Requires: google-cloud-storage

"""

import time
 
from google.cloud import storage
 
 
class GcsManager:

    def __init__(self, config: dict):

        self.bucket_name = config["gcp"]["gcs_bucket"]

        self.client = storage.Client(project=config["gcp"]["project_id"])
 
    def list_files(self, table_name: str) -> list[str]:

        blobs = self.client.list_blobs(self.bucket_name, prefix=f"parquet/{table_name}/")

        return [b.name for b in blobs]
 
    def files_exist(self, table_name: str) -> bool:

        """Kept for backwards compatibility. NOTE: this only checks that

        *some* file exists for the table — once earlier batches have

        landed, this is always True and can't catch a specific batch's

        file failing to transfer. Use batch_file_exists() instead for

        anything that's about to load one specific batch."""

        return len(self.list_files(table_name)) > 0
 
    def batch_file_exists(self, table_name: str, batch_index: int,

                           retries: int = 3, retry_delay_seconds: int = 5) -> bool:

        """Checks that THIS batch's exact file exists in GCS — the file

        load_batch_to_staging() is about to point BigQuery at. Retries a

        few times with a short delay, since an STS operation can report

        done() a moment before the object is listable/gettable.

        """

        blob_name = f"parquet/{table_name}/{table_name}_part{batch_index:04d}.parquet"

        bucket = self.client.bucket(self.bucket_name)

        for attempt in range(1, retries + 1):

            if bucket.blob(blob_name).exists(self.client):

                return True

            if attempt < retries:

                time.sleep(retry_delay_seconds)

        return False
 