"""
parquet_writer.py

Writes one extracted page/batch (see src/extraction/sql_extractor.py's
keyset pagination) to its own compressed Parquet file — this is the
step that gives you the real cost saving (smaller files = less Azure
egress, less GCS storage), and keeps each batch independently
loadable/mergeable so a failure doesn't force redoing the whole table.
"""
import os
import pandas as pd


def write_parquet(df: pd.DataFrame, table_name: str, batch_index: int, output_dir: str = "data/parquet") -> str:
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{table_name}_part{batch_index:04d}.parquet"
    path = os.path.join(output_dir, filename)
    df.to_parquet(path, compression="snappy", index=False)
    return path