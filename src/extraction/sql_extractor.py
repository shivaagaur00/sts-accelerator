import os
 
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
 
load_dotenv()
 
 
class SqlExtractor:
    def __init__(self, config: dict):
        self.source_cfg = config["source"]
        if self.source_cfg.get("type") != "mssql":
            raise ValueError(
                f"Unsupported source.type '{self.source_cfg.get('type')}' — "
                "only 'mssql' is supported."
            )
        self._engine = None
 
    def _get_engine(self):
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
 
    @staticmethod
    def _keyset_where(pk_cols: list[str], last_values: dict | None) -> str:
        """Builds the WHERE clause for 'rows strictly after the last page's
        final key', supporting composite primary keys via row-value
        comparison semantics expressed as an OR-chain (SQL Server has no
        native row-value `>` operator on older engines)."""
        if not last_values:
            return ""
        # (k1 > v1) OR (k1 = v1 AND k2 > v2) OR (k1 = v1 AND k2 = v2 AND k3 > v3) ...
        clauses = []
        for i in range(len(pk_cols)):
            parts = [f"[{pk_cols[j]}] = :pk_{j}" for j in range(i)]
            parts.append(f"[{pk_cols[i]}] > :pk_{i}")
            clauses.append("(" + " AND ".join(parts) + ")")
        return "WHERE " + " OR ".join(clauses)
 
    def extract_batches(self, table_cfg: dict, batch_size: int, resume_after: dict | None = None):
        """Yields (batch_index, DataFrame) pairs, one per page, until the
        table is exhausted. batch_index is 0-based and continues from
        wherever resume_after left off (checkpoint tracks the real index).
 
        For tables with no primary key, table_planner.py already injected
        a synthetic `source_row_id` column into table_cfg["primary_key"]
        and set table_cfg["synthetic_key"] = True — so from here on, a
        no-PK table looks exactly like a PK table and goes through the
        identical keyset-pagination path below. The only extra step is
        making sure the physical column actually exists in SQL Server
        first (see _ensure_synthetic_key_table)."""
        schema = table_cfg["schema"]
        table = table_cfg["name"]
        key_cols = table_cfg["primary_key"]
 
        read_schema, read_table = schema, table
        if table_cfg.get("synthetic_key"):
            read_schema, read_table = self._ensure_synthetic_key_table(schema, table, key_cols[0])
 
        yield from self._extract_batches_by_key(read_schema, read_table, key_cols, batch_size, resume_after)
 
    def _extract_batches_by_key(self, schema: str, table: str, key_cols: list[str],
                                 batch_size: int, resume_after: dict | None):
        """The actual keyset (seek) pagination loop — used identically for
        a real primary key and for a materialized synthetic key. Never
        called with an empty key_cols; extract_batches guarantees one of
        the two exists before reaching here."""
        last_values = resume_after
        batch_index = 0
        order_by = ", ".join(f"[{c}]" for c in key_cols)
 
        while True:
            where_clause = self._keyset_where(key_cols, last_values)
            params = {}
            if last_values:
                params = {
                    f"pk_{i}": (
                        last_values[c].item() if hasattr(last_values[c], "item") else last_values[c]
                    )
                    for i, c in enumerate(key_cols)
                }
 
            query = (
                f"SELECT TOP {batch_size} * FROM [{schema}].[{table}] "
                f"{where_clause} ORDER BY {order_by}"
            )
            df = self._run(query, params)
            if df.empty:
                break
 
            yield batch_index, df
 
            last_row = df.iloc[-1]
            last_values = {
                c: (last_row[c].item() if hasattr(last_row[c], "item") else last_row[c])
                for c in key_cols
            }
            batch_index += 1
 
            if len(df) < batch_size:
                break
 
    def _ensure_synthetic_key_table(self, schema: str, table: str, key_col: str) -> tuple[str, str]:
        """Materializes a stable, physically indexed row-number column for
        a table with no primary key, so it can be batched, resumed, and
        merged with the exact same zero-duplicate guarantees a real PK
        gives — instead of pulling the whole table in one unbounded shot.
 
        Only created once: if the shadow table already exists (e.g. a
        previous run got partway through and this run is resuming), it's
        reused as-is rather than rebuilt, so the row numbers — and
        therefore any in-progress checkpoint — stay stable across runs.
        Safe to call every time; this is a no-op after the first call.
 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) makes no promise about
        WHICH row gets which number — only that numbering is stable once
        materialized into a real table with a real index on it, which is
        exactly what happens here. That materialization (not the ordering
        clause) is what makes the "no duplicates, no gaps" guarantee hold.
        """
        shadow_table = f"__migration_rownum__{table}"
        with self._get_engine().connect() as conn:
            exists = conn.execute(
                text(
                    "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                ),
                {"schema": schema, "table": shadow_table},
            ).first()
 
            if not exists:
                conn.execute(text(
                    f"SELECT *, CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS BIGINT) AS [{key_col}] "
                    f"INTO [{schema}].[{shadow_table}] FROM [{schema}].[{table}]"
                ))
                conn.execute(text(
                    f"CREATE UNIQUE CLUSTERED INDEX [IX_{shadow_table}_{key_col}] "
                    f"ON [{schema}].[{shadow_table}] ([{key_col}])"
                ))
                conn.commit()
 
        return schema, shadow_table
 
    def drop_synthetic_key_table(self, table_cfg: dict) -> None:
        """Called by table_pipeline.py only after a synthetic-key table
        is fully COMPLETED (never on partial failure — the shadow table
        must survive to let the next run resume from its checkpoint)."""
        schema = table_cfg["schema"]
        shadow_table = f"__migration_rownum__{table_cfg['name']}"
        with self._get_engine().connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{schema}].[{shadow_table}]"))
            conn.commit()
 
 
    def _run(self, query: str, params: dict | None = None) -> pd.DataFrame:
        with self._get_engine().connect() as conn:
            return pd.read_sql_query(text(query), conn, params=params or {})
 
 