-- Reference copy only — src/bigquery/bq_control_tables.py creates this
-- dynamically at runtime (schema/partitioning/clustering taken from
-- src/planner/table_planner.py's dynamic plan for the table), so you
-- never hand-maintain per-table DDL here. Kept for visibility into
-- what gets created.
--
-- Staging mirrors the target schema exactly and holds exactly ONE
-- batch's worth of rows at a time (truncated before each batch load)
-- before bq_merge.py MERGEs it into the target table.

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table}_staging`
LIKE `{project}.{dataset}.{table}`;
