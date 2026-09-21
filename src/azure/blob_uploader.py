"""
blob_uploader.py

Uploads a local Parquet file to the Azure Blob container, using the
connection string from .env (never hardcoded).

Requires: azure-storage-blob
  pip install azure-storage-blob
"""
import os
from azure.storage.blob import BlobServiceClient


class BlobUploader:
    def __init__(self, config: dict):
        self.container_name = config["azure"]["container"]
        conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not conn_str:
            raise RuntimeError(
                "AZURE_STORAGE_CONNECTION_STRING not set — check your .env file"
            )
        self.client = BlobServiceClient.from_connection_string(conn_str)

    def upload_file(self, local_path: str, table_name: str) -> str:
        """Uploads to <container>/<table_name>/<filename>, returns the blob path."""
        filename = os.path.basename(local_path)
        blob_path = f"{table_name}/{filename}"
        blob_client = self.client.get_blob_client(container=self.container_name, blob=blob_path)
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        return blob_path

    def list_blobs(self, table_name: str) -> list[str]:
        container_client = self.client.get_container_client(self.container_name)
        return [b.name for b in container_client.list_blobs(name_starts_with=f"{table_name}/")]
