"""FastAPI dependencies — shared across routers."""
from typing import Generator

import duckdb

from config.settings import DUCKDB_PATH


def get_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Yield a read-only DuckDB connection; close after request."""
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        yield con
    finally:
        con.close()
