#!/usr/bin/env python3
"""
CLI runner for the StatsBomb ETL pipeline.

Usage:
  python scripts/run_pipeline.py --download
  python scripts/run_pipeline.py --bronze
  python scripts/run_pipeline.py --silver
  python scripts/run_pipeline.py --gold
  python scripts/run_pipeline.py --all
"""
import sys
import argparse
from pathlib import Path

# Ensure project root is on sys.path
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))


def main():
    parser = argparse.ArgumentParser(
        description="Run StatsBomb ETL pipeline stages"
    )
    parser.add_argument("--download", action="store_true", help="Download raw JSON from StatsBomb")
    parser.add_argument("--bronze",   action="store_true", help="Ingest JSON → Parquet (bronze)")
    parser.add_argument("--silver",   action="store_true", help="Build silver DuckDB tables")
    parser.add_argument("--gold",     action="store_true", help="Build gold aggregation tables")
    parser.add_argument("--all",      action="store_true", help="Run all stages in order")
    parser.add_argument("--force",    action="store_true", help="Force re-run (overwrite existing)")
    args = parser.parse_args()

    if not any([args.download, args.bronze, args.silver, args.gold, args.all]):
        parser.print_help()
        sys.exit(0)

    if args.download or args.all:
        print("=== DOWNLOAD ===")
        from pipeline import downloader
        downloader.run()

    if args.bronze or args.all:
        print("=== BRONZE ===")
        from pipeline import bronze
        bronze.run(force=args.force)

    if args.silver or args.all:
        print("=== SILVER ===")
        from pipeline import silver
        silver.run()

    if args.gold or args.all:
        print("=== GOLD ===")
        from pipeline import gold
        gold.run()

    print("\nDone.")


if __name__ == "__main__":
    main()
