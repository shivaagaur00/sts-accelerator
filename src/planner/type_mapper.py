"""
type_mapper.py

Maps SQL Server column types to BigQuery types, so bq_control_tables.py
can create tables with an explicit, correct schema instead of relying
on autodetect (which occasionally guesses wrong on edge cases like
dates stored as text). SQLite mapping has been removed along with the
rest of the SQLite demo path.
"""

MSSQL_TO_BQ = {
    "int": "INT64", "bigint": "INT64", "smallint": "INT64", "tinyint": "INT64",
    "decimal": "NUMERIC", "numeric": "NUMERIC", "money": "NUMERIC", "smallmoney": "NUMERIC",
    "float": "FLOAT64", "real": "FLOAT64",
    "varchar": "STRING", "nvarchar": "STRING", "char": "STRING", "nchar": "STRING", "text": "STRING", "ntext": "STRING",
    "date": "DATE", "datetime": "TIMESTAMP", "datetime2": "TIMESTAMP",
    "smalldatetime": "TIMESTAMP", "datetimeoffset": "TIMESTAMP", "time": "TIME",
    "bit": "BOOL",
    "uniqueidentifier": "STRING",
    "varbinary": "BYTES", "binary": "BYTES",
}


def map_type(source_type: str) -> str:
    key = source_type.split("(")[0].strip().lower()
    mapped = MSSQL_TO_BQ.get(key)
    if mapped is None:
        # Fall back to STRING rather than failing outright — safer for a
        # first pass, and easy to spot/fix in the BigQuery schema after.
        return "STRING"
    return mapped


def build_bigquery_schema(columns: list[dict]) -> list[dict]:
    """columns: [{'name':..., 'source_type':...}, ...] from schema_reader."""
    return [{"name": col["name"], "type": map_type(col["source_type"])} for col in columns]
