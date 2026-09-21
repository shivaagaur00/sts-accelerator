import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.planner.type_mapper import map_type, build_bigquery_schema


def test_mssql_types():
    assert map_type("varchar(50)") == "STRING"
    assert map_type("int") == "INT64"
    assert map_type("datetime2") == "TIMESTAMP"
    assert map_type("uniqueidentifier") == "STRING"
    assert map_type("bit") == "BOOL"


def test_unknown_type_falls_back_to_string():
    assert map_type("xml") == "STRING"


def test_build_bigquery_schema():
    columns = [{"name": "id", "source_type": "int"}, {"name": "name", "source_type": "varchar(100)"}]
    schema = build_bigquery_schema(columns)
    assert schema == [{"name": "id", "type": "INT64"}, {"name": "name", "type": "STRING"}]


if __name__ == "__main__":
    test_mssql_types()
    test_unknown_type_falls_back_to_string()
    test_build_bigquery_schema()
    print("All tests passed.")
