"""
table_discovery.py

The single source of truth for "what tables exist and what is their
primary key" — read straight from SQL Server's INFORMATION_SCHEMA /
sys catalog views. Nothing here is hardcoded per table: add a table to
the source database and it shows up on the next discovery run with no
code or config change.

SQLite support has been removed entirely — this project only talks to
real SQL Server (Azure SQL / on-prem) via SQLAlchemy + pyodbc.
"""
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class TableDiscovery:
    def __init__(self, config: dict):
        self.source_cfg = config["source"]
        if self.source_cfg.get("type") != "mssql":
            raise ValueError(
                f"Unsupported source.type '{self.source_cfg.get('type')}' — "
                "only 'mssql' is supported."
            )
        self._engine: Engine | None = None

    def _get_engine(self) -> Engine:
        if self._engine is None:
            user = os.environ["AZURE_SQL_USERNAME"]
            pwd = os.environ["AZURE_SQL_PASSWORD"]
            host = self.source_cfg["host"]
            database = self.source_cfg["database"]
            driver = self.source_cfg.get("driver", "ODBC Driver 18 for SQL Server")
            conn_str = (
                f"mssql+pyodbc://{user}:{pwd}@{host}/{database}"
                f"?driver={driver.replace(' ', '+')}"
            )
            self._engine = create_engine(conn_str, pool_pre_ping=True, fast_executemany=True)
        return self._engine

    def list_schemas(self) -> list[str]:
        """Schemas to discover tables from — from settings.yaml, defaults to ['dbo']."""
        return self.source_cfg.get("schemas") or ["dbo"]

    def list_tables(self, schema: str) -> list[str]:
        """Every base table in `schema`, discovered dynamically — no manual list."""
        query = text(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema
            ORDER BY TABLE_NAME
            """
        )
        with self._get_engine().connect() as conn:
            rows = conn.execute(query, {"schema": schema}).fetchall()
        return [r[0] for r in rows]

    def list_all_tables(self) -> list[dict]:
        """[{'schema': 'dbo', 'name': 'orders'}, ...] across every configured schema."""
        out = []
        for schema in self.list_schemas():
            for name in self.list_tables(schema):
                out.append({"schema": schema, "name": name})
        return out

    def get_primary_key(self, table_name: str, schema: str) -> list[str]:
        """Ordered primary-key column(s) for a table, straight from SQL Server.

        Returns [] if the table has no primary key — in that case the
        pipeline falls back to a single-batch (non-resumable) extract for
        that table and logs a warning, since keyset pagination needs a key.
        """
        query = text(
            """
            SELECT KU.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
              ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
             AND TC.TABLE_SCHEMA = KU.TABLE_SCHEMA
            WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND TC.TABLE_NAME = :table
              AND TC.TABLE_SCHEMA = :schema
            ORDER BY KU.ORDINAL_POSITION
            """
        )
        with self._get_engine().connect() as conn:
            rows = conn.execute(query, {"table": table_name, "schema": schema}).fetchall()
        return [r[0] for r in rows]