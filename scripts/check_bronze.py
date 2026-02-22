#!/usr/bin/env python3
"""Validate bronze Parquet files — count rows and check key columns."""
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

import pandas as pd
from config.settings import BRONZE_PATH


def check_layer(layer: str, required_cols: list[str] | None = None) -> None:
    directory = BRONZE_PATH / layer
    files = list(directory.glob("*.parquet"))
    if not files:
        print(f"  [{layer}] ❌ No parquet files found in {directory}")
        return

    total_rows = 0
    for f in sorted(files):
        df = pd.read_parquet(f)
        total_rows += len(df)
        if required_cols:
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                print(f"  [{layer}] ⚠  {f.name}: missing columns {missing}")
    print(f"  [{layer}] ✓  {len(files)} file(s), {total_rows:,} total rows")


def main():
    print("=== Bronze layer validation ===\n")
    check_layer("competitions", required_cols=["competition_id", "season_id", "competition_name"])
    check_layer("matches",      required_cols=["match_id"])
    check_layer("events",       required_cols=["id", "match_id", "type"])
    check_layer("lineups",      required_cols=["match_id", "player_id", "player_name"])
    print("\nDone.")


if __name__ == "__main__":
    main()
