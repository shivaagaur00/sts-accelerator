"""
main.py

Run one table through the whole pipeline manually, without Airflow —
useful for testing in Cloud Shell before trusting it to a scheduled
DAG. The table doesn't need to be listed anywhere: its config
(primary key, load_mode, batching) is discovered live from SQL Server
by src/planner/table_planner.py. Every load is insert-only — there is
no watermark column anywhere in this pipeline.

Usage:
  python main.py --table customers
  python main.py --all                   # migrate every dynamically discovered table
  python main.py --list                  # show every dynamically discovered table
  python main.py --report                # (re)generate the tablewise status report
"""
import argparse
import uuid

import yaml

from src.planner.table_planner import build_table_plan, get_table_plan
from src.pipeline.table_pipeline import run_table
from src.reporting.report_generator import ReportGenerator


def run_one(settings, logging_config, table_cfg, run_id):
    print(f"\n{'=' * 70}")
    print(f"Table:  {table_cfg['schema']}.{table_cfg['name']}  "
          f"(load_mode={table_cfg['load_mode']}, pk={table_cfg['primary_key']})")
    print(f"Run ID: {run_id}")
    result = run_table(settings, logging_config, table_cfg, run_id)
    print(f"Done: {result}")
    return result


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Table name to migrate (discovered dynamically from SQL Server)")
    parser.add_argument("--all", action="store_true", help="Migrate every dynamically discovered table")
    parser.add_argument("--list", action="store_true", help="List every dynamically discovered table and exit")
    parser.add_argument("--report", action="store_true",
                         help="(Re)generate the tablewise status report and exit")
    args = parser.parse_args()

    settings = load_yaml("config/settings.yaml")
    logging_config = load_yaml("config/logging.yaml")

    if args.report:
        result = ReportGenerator(settings, logging_config).generate()
        print(f"Report generated: {result['tables_reported']} tables")
        print(f"  BigQuery table:  {result['bigquery_table']}")
        print(f"  Local snapshot:  {result['local_snapshot']}")
        return

    if args.list:
        for t in build_table_plan(settings):
            print(f"{t['schema']}.{t['name']:30s} load_mode={t['load_mode']:11s} "
                  f"pk={t['primary_key']}")
        return

    if args.all:
        tables = build_table_plan(settings)
        if not tables:
            raise SystemExit("No tables were discovered — check source.schemas in config/settings.yaml.")

        batch_run_id = str(uuid.uuid4())
        print(f"Batch run ID: {batch_run_id}")
        print(f"Migrating {len(tables)} table(s): "
              f"{', '.join(t['schema'] + '.' + t['name'] for t in tables)}")

        succeeded, failed = [], []
        for table_cfg in tables:
            # each table gets its own run_id (like migrate_table.expand() does
            # per Airflow task instance) so checkpoints/audit rows don't collide
            run_id = f"{batch_run_id}:{table_cfg['schema']}.{table_cfg['name']}"
            try:
                run_one(settings, logging_config, table_cfg, run_id)
                succeeded.append(table_cfg["name"])
            except Exception as exc:
                # don't let one table's failure stop the rest of the batch —
                # same all_done/resume-later behavior as the Airflow DAG
                print(f"FAILED | Table={table_cfg['name']} | Reason={exc}")
                failed.append(table_cfg["name"])

        print(f"\n{'=' * 70}")
        print(f"Batch complete: {len(succeeded)} succeeded, {len(failed)} failed")
        if succeeded:
            print(f"  Succeeded: {', '.join(succeeded)}")
        if failed:
            print(f"  Failed:    {', '.join(failed)}")

        report = ReportGenerator(settings, logging_config).generate()
        print(f"Report generated: {report['tables_reported']} tables")
        print(f"  BigQuery table:  {report['bigquery_table']}")
        print(f"  Local snapshot:  {report['local_snapshot']}")

        if failed:
            raise SystemExit(1)
        return

    if not args.table:
        raise SystemExit("Provide --table <name>, --all, or --list")

    table_cfg = get_table_plan(settings, args.table)
    if not table_cfg:
        raise SystemExit(
            f"Table '{args.table}' was not found by dynamic discovery "
            "(check it exists in one of source.schemas, or wasn't excluded "
            "in config/tables_override.yaml)."
        )

    run_id = str(uuid.uuid4())
    run_one(settings, logging_config, table_cfg, run_id)


if __name__ == "__main__":
    main()