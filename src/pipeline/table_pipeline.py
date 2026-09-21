"""
table_pipeline.py
 
The one place the full extract -> parquet -> Azure -> STS -> BigQuery
merge chain is implemented, so main.py, migration_dag.py and
incremental_dag.py all run the exact same logic instead of three
copies drifting apart. Everything it needs (primary key, load_mode,
batch size...) comes from the dynamic table_cfg built by
src/planner/table_planner.py — there is nothing table-specific
hardcoded here.
 
Every merge is insert-only (see src/bigquery/bq_merge.py) — there is
no watermark column and no UPDATE branch anywhere in this pipeline,
since the source data is static and only ever grows by new rows.
 
Failure/resume strategy:
  - Before starting, check CheckpointManager for this table. If the
    last run left it IN_PROGRESS or FAILED, resume extraction right
    after the last committed primary key instead of from batch 0.
  - After each batch is loaded to staging and MERGEd into the target,
    commit a checkpoint (IN_PROGRESS with that batch's last PK).
  - On success of the whole table, mark COMPLETED. On any exception,
    leave the last good checkpoint in place (already IN_PROGRESS from
    the last successful batch) and record the failure in the audit
    table — the next cycle picks up exactly where this one stopped.
  - Because the merge is insert-only and matches on primary key,
    re-running (resuming) a batch that already landed is always a
    safe no-op — nothing to guard with a watermark.
"""
import datetime
 
from src.extraction.sql_extractor import SqlExtractor
from src.extraction.parquet_writer import write_parquet
from src.azure.blob_uploader import BlobUploader
from src.sts.sts_job_manager import StsJobManager
from src.gcs.gcs_manager import GcsManager
from src.bigquery.bq_control_tables import BqControlTables
from src.bigquery.bq_merge import BqMerge
from src.metadata.metadata_manager import MetadataManager
from src.metadata.checkpoint_manager import CheckpointManager
 
 
def run_table(settings: dict, logging_config: dict, table_cfg: dict, run_id: str) -> dict:
    table_name = table_cfg["name"]
    metadata_cfg = settings.get("metadata", {})
    gcp_cfg = {**settings["gcp"], **metadata_cfg}
 
    metadata = MetadataManager(logging_config, settings["gcp"])
    control_tables = BqControlTables(settings)
    checkpoint = CheckpointManager(settings, metadata_cfg.get("checkpoint_table", "migration_checkpoint"))
    batch_size = settings.get("extraction", {}).get("batch_size", 1000)
 
    control_tables.ensure_target_table(table_cfg)
    control_tables.ensure_staging_table(table_cfg)
    control_tables.ensure_log_table(metadata_cfg.get("log_table", "migration_pipeline_logs"))
    control_tables.ensure_checkpoint_table(metadata_cfg.get("checkpoint_table", "migration_checkpoint"))
    control_tables.ensure_audit_table(metadata_cfg.get("audit_table", "migration_audit"))
 
    resume_after, start_batch_index, is_resumed = checkpoint.resume_point(table_name)
    started_at = datetime.datetime.utcnow()
    batches_processed = 0
    rows_processed = 0
    last_pk = resume_after
 
    metadata.log_stage_start(run_id, table_name, "pipeline", schema_name=table_cfg["schema"])
 
    try:
        extractor = SqlExtractor(settings)
        uploader = BlobUploader(settings)
        sts = StsJobManager(settings)
        gcs = GcsManager(settings)
        merger = BqMerge(settings)
 
        for batch_index, df in extractor.extract_batches(table_cfg, batch_size, resume_after=resume_after):
            real_batch_index = start_batch_index + batch_index
 
            metadata.log_stage_start(run_id, table_name, "extraction", table_cfg["schema"])
            parquet_path = write_parquet(df, table_name, real_batch_index)
            metadata.log_stage_end(run_id, table_name, "extraction", "SUCCESS",
                                    schema_name=table_cfg["schema"], batch_index=real_batch_index,
                                    rows_processed=len(df))
 
            metadata.log_stage_start(run_id, table_name, "upload", table_cfg["schema"])
            blob_path = uploader.upload_file(parquet_path, table_name)
            metadata.log_stage_end(run_id, table_name, "upload", "SUCCESS",
                                    schema_name=table_cfg["schema"], batch_index=real_batch_index,
                                    extra={"blob_path": blob_path})
 
            metadata.log_stage_start(run_id, table_name, "transfer", table_cfg["schema"])
            transfer_result = sts.run_transfer_for_table(table_name)
            metadata.log_stage_end(run_id, table_name, "transfer", transfer_result["status"],
                                    schema_name=table_cfg["schema"], batch_index=real_batch_index,
                                    extra=transfer_result)
            if transfer_result["status"] != "SUCCESS":
                raise RuntimeError(
                    f"Transfer for {table_name} batch {real_batch_index} did not complete "
                    f"(status={transfer_result['status']}): {transfer_result}"
                )
            if not gcs.batch_file_exists(table_name, real_batch_index):
                raise RuntimeError(
                    f"{table_name}_part{real_batch_index:04d}.parquet is missing in GCS "
                    f"after transfer reported success — refusing to load a batch that isn't there."
                )
 
            metadata.log_stage_start(run_id, table_name, "load", table_cfg["schema"])
            merger.load_batch_to_staging(table_cfg, real_batch_index)
            merge_result = merger.merge_batch_into_target(table_cfg)
            metadata.log_stage_end(run_id, table_name, "load", "SUCCESS",
                                    schema_name=table_cfg["schema"], batch_index=real_batch_index,
                                    rows_processed=len(df), extra=merge_result)
 
            pk_cols = table_cfg["primary_key"]
            if pk_cols:
                last_row = df.iloc[-1]
                last_pk = {c: last_row[c] for c in pk_cols}
            checkpoint.commit_batch(run_id, table_cfg, real_batch_index, last_pk)
 
            batches_processed += 1
            rows_processed += len(df)
 
        checkpoint.mark_completed(run_id, table_cfg, start_batch_index + batches_processed - 1, last_pk)
 
        if table_cfg.get("synthetic_key"):
            # Only safe to clean up now that the table is fully COMPLETED —
            # if this ran after every batch instead, a run that failed
            # partway through would lose the shadow table (and therefore
            # the stable row numbering) its checkpoint depends on to resume.
            try:
                extractor.drop_synthetic_key_table(table_cfg)
            except Exception as cleanup_err:
                print(f"[table_pipeline] WARNING: could not drop shadow table "
                      f"for {table_name}: {cleanup_err}")
 
        metadata.log_stage_end(run_id, table_name, "pipeline", "SUCCESS",
                                schema_name=table_cfg["schema"], rows_processed=rows_processed)
 
        _write_audit_row(control_tables, metadata_cfg, run_id, table_cfg, "SUCCESS",
                          batches_processed, rows_processed, start_batch_index, started_at, None)
        return {"table": table_name, "status": "SUCCESS",
                "batches_processed": batches_processed, "rows_processed": rows_processed}
 
    except Exception as e:
        checkpoint.mark_failed(run_id, table_cfg, start_batch_index + max(batches_processed - 1, 0), last_pk)
        metadata.log_stage_end(run_id, table_name, "pipeline", "FAILED",
                                schema_name=table_cfg["schema"], error_message=str(e))
        _write_audit_row(control_tables, metadata_cfg, run_id, table_cfg, "FAILED",
                          batches_processed, rows_processed, start_batch_index, started_at, str(e))
        raise
 
 
def _write_audit_row(control_tables: BqControlTables, metadata_cfg: dict, run_id: str, table_cfg: dict,
                      outcome: str, batches_processed: int, rows_processed: int,
                      resumed_from_batch: int, started_at: datetime.datetime, error_message: str | None) -> None:
    audit_table = metadata_cfg.get("audit_table", "migration_audit")
    table_ref = f"{control_tables.project_id}.{control_tables.dataset}.{audit_table}"
    row = {
        "run_id": run_id,
        "table_name": table_cfg["name"],
        "schema_name": table_cfg["schema"],
        "load_mode": table_cfg["load_mode"],
        "outcome": "RESUMED_SUCCESS" if (outcome == "SUCCESS" and resumed_from_batch > 0) else outcome,
        "batches_processed": batches_processed,
        "rows_processed": rows_processed,
        "resumed_from_batch": resumed_from_batch,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.datetime.utcnow().isoformat(),
        "error_message": error_message,
    }
    errors = control_tables.client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"[table_pipeline] WARNING: audit row insert failed: {errors}")
 
 