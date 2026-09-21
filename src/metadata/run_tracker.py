"""
run_tracker.py

Reads the raw per-event JSON objects written by metadata_manager (one
object per event, under gcs://<bucket>/pipeline_logs/raw/<run_id>/) and
answers "is this run done, and did it succeed" across every table and
stage. Also exposes a BigQuery-backed summary for anyone who'd rather
query the migration_pipeline_logs table directly with SQL.
"""
import json
import os
import glob

from google.cloud import storage as gcs_storage

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


class RunTracker:
    def __init__(self, logging_config: dict, gcp_config: dict | None = None):
        self.gcp_config = gcp_config or {}
        raw_log_path = logging_config.get("raw_log_path")
        if not raw_log_path:
            bucket = self.gcp_config.get("gcs_bucket")
            raw_log_path = f"gcs://{bucket}/pipeline_logs/raw" if bucket else None
        self.raw_log_path = raw_log_path
        self.log_table = (self.gcp_config.get("log_table")
                           or logging_config.get("bq_log_table")
                           or "migration_pipeline_logs")

    def get_events(self, run_id: str) -> list[dict]:
        if not self.raw_log_path:
            return []
        if self.raw_log_path.startswith("gcs://"):
            return self._get_events_gcs(run_id)
        return self._get_events_local(run_id)

    def _get_events_local(self, run_id: str) -> list[dict]:
        directory = os.path.join(self.raw_log_path, run_id)
        events = []
        for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
            with open(path, encoding="utf-8") as f:
                events.append(json.load(f))
        return events

    def _get_events_gcs(self, run_id: str) -> list[dict]:
        bucket_name, prefix = self.raw_log_path[len("gcs://"):].split("/", 1)
        client = gcs_storage.Client()
        blobs = client.list_blobs(bucket_name, prefix=f"{prefix.rstrip('/')}/{run_id}/")
        return [json.loads(b.download_as_text()) for b in blobs]


    def find_all_run_ids(self) -> list[str]:
        """Used by log_collector to build a report across many runs."""
        if not self.raw_log_path:
            return []
        if self.raw_log_path.startswith("gcs://"):
            bucket_name, prefix = self.raw_log_path[len("gcs://"):].split("/", 1)
            client = gcs_storage.Client()
            blobs = client.list_blobs(bucket_name, prefix=f"{prefix.rstrip('/')}/", delimiter="/")
            list(blobs)  # force iteration so .prefixes is populated
            return [p.rstrip("/").rsplit("/", 1)[-1] for p in blobs.prefixes]
        run_dirs = glob.glob(os.path.join(self.raw_log_path, "*"))
        return [os.path.basename(d) for d in run_dirs if os.path.isdir(d)]
