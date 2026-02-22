"""Shared utility functions for the ETL pipeline."""
from pathlib import Path
from typing import Any
import duckdb
import numpy as np
import pandas as pd
from config.settings import BRONZE_PATH, DUCKDB_PATH


# ── DuckDB connection ──────────────────────────────────────────────────────────

def get_db_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection to the gold database."""
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


# ── Location unpacking ─────────────────────────────────────────────────────────

def unpack_location(series: pd.Series, prefix: str = "location") -> pd.DataFrame:
    """
    Unpack a Series of StatsBomb location fields into x/y/z columns.
    Each value may be a Python list, numpy array, or None/NaN.
    Returns a DataFrame with columns: {prefix}_x, {prefix}_y (and optionally _z).
    """
    def extract(v):
        if v is None:
            return [None, None, None]
        if isinstance(v, float) and np.isnan(v):
            return [None, None, None]
        if isinstance(v, (list, np.ndarray)):
            arr = list(v)
            x = float(arr[0]) if len(arr) > 0 and arr[0] is not None else None
            y = float(arr[1]) if len(arr) > 1 and arr[1] is not None else None
            z = float(arr[2]) if len(arr) > 2 and arr[2] is not None else None
            return [x, y, z]
        return [None, None, None]

    unpacked = series.apply(extract)
    result = pd.DataFrame(unpacked.tolist(),
                          index=series.index,
                          columns=[f"{prefix}_x", f"{prefix}_y", f"{prefix}_z"])
    # Drop z column if all null
    if result[f"{prefix}_z"].isna().all():
        result = result.drop(columns=[f"{prefix}_z"])
    return result


# ── Bronze reader ──────────────────────────────────────────────────────────────

def read_all_bronze(layer: str) -> pd.DataFrame:
    """Read all Parquet files from BRONZE_PATH/<layer>/ and concatenate."""
    directory = BRONZE_PATH / layer
    files = list(directory.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {directory}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)