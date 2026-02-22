#!/usr/bin/env python3
"""Validate DuckDB silver/gold tables with sample queries."""
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from pipeline.utils import get_db_connection


TABLES = [
    "dim_competition",
    "dim_match",
    "dim_team",
    "dim_player",
    "fact_events",
    "fact_passes",
    "fact_shots",
    "fact_carries",
    "fact_lineups",
    "bridge_shot_freeze_frame",
    "gold_xg_timeline",
    "gold_pass_network_nodes",
    "gold_pass_network_edges",
    "gold_shot_map",
    "gold_formation_positions",
    "gold_team_stats",
]


def main():
    print("=== Silver / Gold layer validation ===\n")
    con = get_db_connection(read_only=True)

    all_ok = True
    for table in TABLES:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:40s} {count:>8,} rows  ✓")
        except Exception as e:
            print(f"  {table:40s} ❌ {e}")
            all_ok = False

    print()

    # Join validation: fact_passes → dim_match
    print("--- Join checks ---")
    try:
        res = con.execute("""
            SELECT COUNT(*) FROM fact_passes p
            JOIN dim_match m ON p.match_id = m.match_id
        """).fetchone()[0]
        print(f"  fact_passes ⋈ dim_match: {res:,} rows  ✓")
    except Exception as e:
        print(f"  fact_passes ⋈ dim_match: ❌ {e}")
        all_ok = False

    # xG timeline sanity
    try:
        res = con.execute("""
            SELECT COUNT(DISTINCT match_id) FROM gold_xg_timeline
        """).fetchone()[0]
        print(f"  gold_xg_timeline covers {res} matches  ✓")
    except Exception as e:
        print(f"  gold_xg_timeline: ❌ {e}")
        all_ok = False

    con.close()
    print("\n" + ("All checks passed ✓" if all_ok else "Some checks failed ❌"))


if __name__ == "__main__":
    main()
