import os
 
import yaml
 
from src.discovery.table_discovery import TableDiscovery
from src.discovery.schema_reader import SchemaReader
 
 
def _load_overrides(overrides_path: str) -> dict:
    if not overrides_path or not os.path.exists(overrides_path):
        return {}
    with open(overrides_path) as f:
        data = yaml.safe_load(f) or {}
    return {o["name"]: o for o in data.get("overrides", []) if o.get("name")}
 
 
def _dump_table_plan(plan: list[dict], path: str = "config/tables.yaml") -> None:
    """Auto-generated, read-only snapshot of the plan just built — for
    humans to look at, never for the pipeline to read back in. Rewritten
    from scratch every time build_table_plan() runs, so it can never go
    stale or drift from what SQL Server actually has. Safe to delete;
    it just reappears on the next run. Best-effort: a write failure here
    (e.g. read-only filesystem in some deploy environments) must never
    break the actual migration."""
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        snapshot = {
            "_generated_by": "src/planner/table_planner.py — rewritten on every run, do not hand-edit",
            "tables": [
                {
                    "schema": t["schema"],
                    "name": t["name"],
                    "primary_key": t["primary_key"],
                    "synthetic_key": t["synthetic_key"],
                    "load_mode": t["load_mode"],
                }
                for t in plan
            ],
        }
        with open(path, "w") as f:
            yaml.safe_dump(snapshot, f, sort_keys=False)
    except OSError as e:
        print(f"[table_planner] WARNING: could not write {path} snapshot: {e}")
 
 
def build_table_plan(config: dict) -> list[dict]:
    """The one function every entrypoint (DAGs, main.py) calls to get the
    live, dynamic list of tables to migrate."""
    discovery = TableDiscovery(config)
    schema_reader = SchemaReader(config)
    overrides = _load_overrides(config.get("overrides_file"))
 
    plan = []
    for entry in discovery.list_all_tables():
        table_name, schema = entry["name"], entry["schema"]
        override = overrides.get(table_name, {})
 
        if override.get("exclude"):
            continue
 
        primary_key = discovery.get_primary_key(table_name, schema)
        columns = schema_reader.get_columns(table_name, schema)
 
        synthetic_key = False
        if not primary_key:
            # No real primary key -> can't safely page, resume, or MERGE
            # without one. Rather than falling back to an unbounded
            # single-shot pull (breaks on large tables) or refusing to
            # migrate the table at all, inject a synthetic, physically
            # materialized sequential column (source_row_id) that the
            # extractor creates once in SQL Server and treats exactly
            # like a real primary key from here on — same batching, same
            # resumability, same zero-duplicate MERGE guarantee. See
            # SqlExtractor._ensure_synthetic_key_table for how it's built.
            synthetic_col = "source_row_id"
            existing_names = {c["name"].lower() for c in columns}
            if synthetic_col.lower() in existing_names:
                # Extremely unlikely, but don't silently collide with a
                # real column if this table happens to already have one
                # named source_row_id.
                synthetic_col = "_migration_row_id"
            columns = columns + [{"name": synthetic_col, "source_type": "bigint"}]
            primary_key = [synthetic_col]
            synthetic_key = True
 
        load_mode = override.get("load_mode") or "full"
 
        plan.append({
            "name": table_name,
            "schema": schema,
            "primary_key": primary_key,
            "synthetic_key": synthetic_key,
            "columns": columns,
            "load_mode": load_mode,
            "source_query": f"SELECT * FROM [{schema}].[{table_name}]",
        })
 
    _dump_table_plan(plan, config.get("tables_snapshot_file", "config/tables.yaml"))
    return plan
 
 
def get_table_plan(config: dict, table_name: str) -> dict | None:
    """Convenience for main.py --table <name>: builds the full dynamic plan
    and returns just the one table's entry."""
    return next((t for t in build_table_plan(config) if t["name"] == table_name), None)