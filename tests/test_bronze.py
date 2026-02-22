"""Tests for bronze layer Parquet files."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest

from config.settings import BRONZE_PATH


def _parquet_files(layer: str) -> list[Path]:
    directory = BRONZE_PATH / layer
    return list(directory.glob("*.parquet"))


# ── Competitions ────────────────────────────────────────────────────────────────

class TestBronzeCompetitions:
    def test_file_exists(self):
        files = _parquet_files("competitions")
        assert len(files) > 0, "No competition parquet files found"

    def test_required_columns(self):
        df = pd.read_parquet(_parquet_files("competitions")[0])
        for col in ["competition_id", "season_id", "competition_name"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_has_rows(self):
        df = pd.concat([pd.read_parquet(f) for f in _parquet_files("competitions")])
        assert len(df) > 0


# ── Matches ─────────────────────────────────────────────────────────────────────

class TestBronzeMatches:
    def test_file_exists(self):
        files = _parquet_files("matches")
        assert len(files) > 0, "No matches parquet files found"

    def test_has_wc_2022(self):
        files = _parquet_files("matches")
        # Expect at least one file named 43_106.parquet (WC 2022)
        names = [f.name for f in files]
        assert "43_106.parquet" in names, f"WC 2022 match file not found; got: {names}"

    def test_match_count(self):
        df = pd.concat([pd.read_parquet(f) for f in _parquet_files("matches")])
        # WC 2022 has 64 matches
        assert len(df) >= 64


# ── Events ──────────────────────────────────────────────────────────────────────

class TestBronzeEvents:
    def test_files_exist(self):
        files = _parquet_files("events")
        assert len(files) >= 64, f"Expected >= 64 event files, got {len(files)}"

    def test_required_columns(self):
        files = _parquet_files("events")
        df = pd.read_parquet(files[0])
        for col in ["id", "match_id", "type"]:
            assert col in df.columns or any(
                c.startswith("type") for c in df.columns
            ), f"Missing column: {col}"

    def test_no_empty_files(self):
        for f in _parquet_files("events"):
            df = pd.read_parquet(f)
            assert len(df) > 0, f"Empty event file: {f.name}"


# ── Lineups ─────────────────────────────────────────────────────────────────────

class TestBronzeLineups:
    def test_files_exist(self):
        files = _parquet_files("lineups")
        assert len(files) >= 64

    def test_required_columns(self):
        files = _parquet_files("lineups")
        df = pd.read_parquet(files[0])
        for col in ["match_id", "player_id", "player_name"]:
            assert col in df.columns, f"Missing column: {col}"
