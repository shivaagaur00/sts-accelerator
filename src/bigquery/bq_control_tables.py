"""
bq_control_tables.py

Ensures every BigQuery object the pipeline needs exists, all built
dynamically from what's discovered/planned at runtime — nothing here
is a hand-maintained DDL script:

  - the dataset
  - each table's TARGET table       (schema from type_mapper — plain,
                                      unpartitioned, unclustered)
  - each table's STAGING table      (mirrors the target, holds one batch
                                      at a time before MERGE)
  - migration_pipeline_logs         (raw stage-event log, as a queryable
                                      table — see src/metadata/metadata_manager.py)
  - migration_checkpoint            (last committed primary key per table,
                                      used to resume failed/partial runs)
  - migration_audit                 (one row per table per run: final
                                      outcome, rows processed, timing)

All CREATE TABLE IF NOT EXISTS, so calling these repeatedly is safe.
"""
from google.cloud import bigquery

from src.planner.type_mapper import build_bigquery_schema


class BqControlTables:
    def __init__(self, config: dict):
        self.project_id = config["gcp"]["project_id"]
        self.dataset = config["gcp"]["bq_dataset"]
        self.client = bigquery.Client(project=self.project_id)

    def _table_ref(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset}.{table_name}"

    def ensure_dataset(self) -> None:
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            self.client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    def _ensure_data_table(self, table_name: str, table_cfg: dict) -> None:
        table_ref = self._table_ref(table_name)
        try:
            self.client.get_table(table_ref)
            return
        except Exception:
            pass

        schema = [
            bigquery.SchemaField(col["name"], col["type"])
            for col in build_bigquery_schema(table_cfg["columns"])
        ]
        # Plain table: no time partitioning, no clustering.
        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table, exists_ok=True)

    def ensure_target_table(self, table_cfg: dict) -> str:
        self.ensure_dataset()
        self._ensure_data_table(table_cfg["name"], table_cfg)
        return self._table_ref(table_cfg["name"])

    def ensure_staging_table(self, table_cfg: dict) -> str:
        self.ensure_dataset()
        staging_name = f"{table_cfg['name']}_staging"
        self._ensure_data_table(staging_name, table_cfg)
        return self._table_ref(staging_name)

    def ensure_log_table(self, log_table_name: str) -> str:
        self.ensure_dataset()
        table_ref = self._table_ref(log_table_name)
        schema = [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("schema_name", "STRING"),
            bigquery.SchemaField("stage", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("batch_index", "INT64"),
            bigquery.SchemaField("rows_processed", "INT64"),
            bigquery.SchemaField("bytes_processed", "INT64"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("finished_at", "TIMESTAMP"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("extra_json", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="started_at"
        )
        self.client.create_table(table, exists_ok=True)
        return table_ref

    def ensure_checkpoint_table(self, checkpoint_table_name: str) -> str:
        self.ensure_dataset()
        table_ref = self._table_ref(checkpoint_table_name)
        schema = [
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_name", "STRING"),
            bigquery.SchemaField("last_pk_json", "STRING"),
            bigquery.SchemaField("last_batch_index", "INT64"),
            bigquery.SchemaField("status", "STRING"),  # IN_PROGRESS | COMPLETED | FAILED
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table, exists_ok=True)
        return table_ref

    def ensure_audit_table(self, audit_table_name: str) -> str:
        self.ensure_dataset()
        table_ref = self._table_ref(audit_table_name)
        schema = [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("schema_name", "STRING"),
            bigquery.SchemaField("load_mode", "STRING"),
            bigquery.SchemaField("outcome", "STRING"),  # SUCCESS | FAILED | RESUMED_SUCCESS
            bigquery.SchemaField("batches_processed", "INT64"),
            bigquery.SchemaField("rows_processed", "INT64"),
            bigquery.SchemaField("resumed_from_batch", "INT64"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("finished_at", "TIMESTAMP"),
            bigquery.SchemaField("error_message", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="started_at"
        )
        self.client.create_table(table, exists_ok=True)
        return table_ref
