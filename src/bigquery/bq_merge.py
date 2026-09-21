"""
bq_merge.py

INSERT-ONLY. This pipeline's source data is static — existing rows
never change, they only ever get new rows appended. So every table
lands in BigQuery through the same path: load one batch's Parquet
file into that table's staging table, then MERGE it into the target
matching on the table's real (possibly composite) primary key, with
NO "WHEN MATCHED" branch at all. Rows already present in the target
are left completely untouched; only rows not yet present (by primary
key) get inserted.

This is still done as a MERGE rather than a plain INSERT because it's
what makes the failure-recovery story safe:

  If a batch fails partway through a table, the next run resumes
  extraction from the last checkpointed primary key (see
  src/metadata/checkpoint_manager.py) and re-runs this same
  insert-only MERGE forward from there. If a batch had actually
  landed before the failure was recorded, re-running it is a no-op
  (every row in it already matches on primary key, so nothing is
  inserted again) rather than creating duplicates — at-least-once
  batch delivery is always safe.

There is no watermark column anywhere in this design — it isn't
needed for insert-only behavior, since "already exists by primary
key" is the only check a pure-insert merge requires.
"""
from google.cloud import bigquery


class BqMerge:
    def __init__(self, config: dict):
        self.project_id = config["gcp"]["project_id"]
        self.dataset = config["gcp"]["bq_dataset"]
        self.gcs_bucket = config["gcp"]["gcs_bucket"]
        self.client = bigquery.Client(project=self.project_id)

    def load_batch_to_staging(self, table_cfg: dict, batch_index: int) -> str:
        """Loads exactly this batch's Parquet file from GCS into the
        table's staging table, truncating staging first (it only ever
        holds one batch at a time)."""
        table_name = table_cfg["name"]
        staging_ref = f"{self.project_id}.{self.dataset}.{table_name}_staging"
        uri = f"gs://{self.gcs_bucket}/parquet/{table_name}/{table_name}_part{batch_index:04d}.parquet"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = self.client.load_table_from_uri(uri, staging_ref, job_config=job_config)
        load_job.result()
        return staging_ref

    def merge_batch_into_target(self, table_cfg: dict) -> dict:
        table_name = table_cfg["name"]
        pk_cols = table_cfg["primary_key"]

        if not pk_cols:
            raise ValueError(
                f"'{table_name}' has no primary key — cannot MERGE safely."
            )

        columns = [c["name"] for c in table_cfg["columns"]]

        target_ref = f"`{self.project_id}.{self.dataset}.{table_name}`"
        staging_ref = f"`{self.project_id}.{self.dataset}.{table_name}_staging`"

        on_clause = " AND ".join(
            f"T.`{col}` = S.`{col}`"
            for col in pk_cols
        )

        insert_columns = ", ".join(
            f"`{col}`" for col in columns
        )

        insert_values = ", ".join(
            f"S.`{col}`" for col in columns
        )

        # No WHEN MATCHED branch at all — matched rows (already migrated,
        # by primary key) are left untouched. Only new rows get inserted.
        query = f"""
        MERGE {target_ref} T
        USING {staging_ref} S
        ON {on_clause}
        WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
        """
        job = self.client.query(query)
        job.result()

        return {
            "table": target_ref,
            "merge_status": "SUCCESS",
            "rows_affected": job.num_dml_affected_rows,
        }