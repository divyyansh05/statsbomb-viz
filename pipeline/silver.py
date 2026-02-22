"""
Silver layer: builds all dimension and fact tables in DuckDB from bronze Parquet.
Idempotent — drops and recreates each table.
Uses vectorised pandas operations throughout — no row-by-row iteration.
"""
import numpy as np
import pandas as pd
from pipeline.logger import get_logger
from pipeline.utils import get_db_connection, unpack_location
from config.settings import BRONZE_PATH

log = get_logger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _read_bronze(layer: str) -> pd.DataFrame:
    directory = BRONZE_PATH / layer
    files = list(directory.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {directory}")

    frames = []
    for f in files:
        df = pd.read_parquet(f)
        if layer in ("events", "lineups") and "match_id" not in df.columns:
            df["match_id"] = int(f.stem)
        if layer == "matches":
            df = _normalise_matches(df)
        if layer == "events":
            df = _normalise_events(df)
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    return df

def _normalise_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column names for match files — WC flat vs PL nested."""
    rename = {
        "competition.competition_id":   "competition_id",
        "competition.competition_name": "competition_name",
        "competition.country_name":     "country_name",
        "season.season_id":             "season_id",
        "season.season_name":           "season_name",
        "home_team.home_team_id":       "home_team_id",
        "home_team.home_team_name":     "home_team",
        "away_team.away_team_id":       "away_team_id",
        "away_team.away_team_name":     "away_team",
        "competition_stage.name":       "competition_stage",
        "stadium.name":                 "stadium",
        "referee.name":                 "referee",
    }
    actual = {k: v for k, v in rename.items() if k in df.columns}
    return df.rename(columns=actual)

def _normalise_events(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column names across WC (flat) and PL (dot-notation) bronze files."""
    rename = {
        # Event type
        "type.name":              "type",
        "type.id":                "type_id",
        # Team
        "team.name":              "team",
        "team.id":                "team_id",
        # Player
        "player.name":            "player",
        "player.id":              "player_id",
        # Position
        "position.name":          "position",
        "position.id":            "position_id",
        # Play pattern
        "play_pattern.name":      "play_pattern",
        "play_pattern.id":        "play_pattern_id",
        # Possession team
        "possession_team.id":     "possession_team_id",
        "possession_team.name":   "possession_team",
        # Pass
        "pass.end_location":      "pass_end_location",
        "pass.length":            "pass_length",
        "pass.angle":             "pass_angle",
        "pass.height.name":       "pass_height",
        "pass.height.id":         "pass_height_id",
        "pass.body_part.name":    "pass_body_part",
        "pass.body_part.id":      "pass_body_part_id",
        "pass.outcome.name":      "pass_outcome",
        "pass.outcome.id":        "pass_outcome_id",
        "pass.type.name":         "pass_type",
        "pass.type.id":           "pass_type_id",
        "pass.technique.name":    "pass_technique",
        "pass.technique.id":      "pass_technique_id",
        "pass.recipient.id":      "pass_recipient_id",
        "pass.recipient.name":    "pass_recipient",
        "pass.cross":             "pass_cross",
        "pass.switch":            "pass_switch",
        "pass.through_ball":      "pass_through_ball",
        "pass.shot_assist":       "pass_shot_assist",
        "pass.goal_assist":       "pass_goal_assist",
        "pass.assisted_shot_id":  "pass_assisted_shot_id",
        # Shot
        "shot.end_location":      "shot_end_location",
        "shot.statsbomb_xg":      "shot_statsbomb_xg",
        "shot.outcome.name":      "shot_outcome",
        "shot.outcome.id":        "shot_outcome_id",
        "shot.body_part.name":    "shot_body_part",
        "shot.body_part.id":      "shot_body_part_id",
        "shot.technique.name":    "shot_technique",
        "shot.technique.id":      "shot_technique_id",
        "shot.type.name":         "shot_type",
        "shot.type.id":           "shot_type_id",
        "shot.first_time":        "shot_first_time",
        "shot.key_pass_id":       "shot_key_pass_id",
        "shot.freeze_frame":      "shot_freeze_frame",
        # Carry
        "carry.end_location":     "carry_end_location",
    }
    actual_rename = {k: v for k, v in rename.items() if k in df.columns}
    return df.rename(columns=actual_rename)


def _drop_create(con, table: str, df: pd.DataFrame) -> None:
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    log.info(f"  {table}: {count:,} rows")


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Return column if exists, else a null Series of same length."""
    if name not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype=object)
    col = df[name]
    # Handle duplicate columns — pandas returns DataFrame when column name is duplicated
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    return col

def _bool_col(series: pd.Series) -> pd.Series:
    return series.infer_objects(copy=False).fillna(False).astype(bool)


def _int_col(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


# ── Dimension tables ───────────────────────────────────────────────────────────

def build_dim_competition(con) -> None:
    df = _read_bronze("competitions")
    out = df[["competition_id", "season_id", "competition_name",
              "season_name", "country_name"]].drop_duplicates()
    _drop_create(con, "dim_competition", out)


def build_dim_match(con) -> None:
    df = _read_bronze("matches")
    out = pd.DataFrame({
        "match_id":       df["match_id"],
        "competition_id": _col(df, "competition_id"),
        "season_id":      _col(df, "season_id"),
        "match_date":     pd.to_datetime(df["match_date"]),
        "kick_off":       _col(df, "kick_off"),
        "home_team":      _col(df, "home_team"),
        "away_team":      _col(df, "away_team"),
        "home_score":     _int_col(df["home_score"]),
        "away_score":     _int_col(df["away_score"]),
        "stadium":        _col(df, "stadium"),
        "referee":        _col(df, "referee"),
        "stage":          _col(df, "competition_stage"),
        "match_week":     _int_col(df["match_week"]),
    }).drop_duplicates(subset=["match_id"])
    _drop_create(con, "dim_match", out)


def build_dim_team(con) -> None:
    df = _read_bronze("matches")
    home = _col(df, "home_team")
    away = _col(df, "away_team")
    teams = pd.Series(pd.concat([home, away]).unique()).dropna().sort_values()
    out = pd.DataFrame({"team_name": teams.values,
                        "team_id":   range(1, len(teams) + 1)})
    _drop_create(con, "dim_team", out)

def build_dim_player(con) -> None:
    df = _read_bronze("lineups")
    out = df[["player_id", "player_name"]].drop_duplicates(subset=["player_id"]).copy()
    out["country"] = _col(df, "country")
    _drop_create(con, "dim_player", out)


# ── Fact tables ────────────────────────────────────────────────────────────────

def build_fact_events(con, df: pd.DataFrame) -> None:
    loc = unpack_location(df["location"])

    out = pd.DataFrame({
        "event_id":           df["id"],
        "match_id":           df["match_id"],
        "index":              _int_col(df["index"]),
        "period":             _int_col(df["period"]),
        "timestamp":          df["timestamp"],
        "minute":             _int_col(df["minute"]),
        "second":             _int_col(df["second"]),
        "type":               _col(df, "type"),
        "player_id":          _int_col(_col(df, "player_id")),
        "player":             _col(df, "player"),
        "team_id":            _int_col(_col(df, "team_id")),
        "team":               _col(df, "team"),
        "location_x":         loc["location_x"],
        "location_y":         loc["location_y"],
        "duration":           pd.to_numeric(_col(df, "duration"), errors="coerce"),
        "under_pressure":     _bool_col(_col(df, "under_pressure")),
        "out":                _bool_col(_col(df, "out")),
        "play_pattern":       _col(df, "play_pattern"),
        "possession":         _int_col(_col(df, "possession")),
        "possession_team_id": _int_col(_col(df, "possession_team_id")),
        "position":           _col(df, "position"),
    })
    _drop_create(con, "fact_events", out)


def build_fact_passes(con, df: pd.DataFrame) -> None:
    passes = df[df["type"] == "Pass"].copy().reset_index(drop=True)

    loc     = unpack_location(passes["location"])
    end_loc = unpack_location(_col(passes, "pass_end_location"), prefix="end_location")

    out = pd.DataFrame({
        "event_id":        passes["id"],
        "match_id":        passes["match_id"],
        "player_id":       _int_col(_col(passes, "player_id")),
        "team_id":         _int_col(_col(passes, "team_id")),
        "period":          _int_col(passes["period"]),
        "minute":          _int_col(passes["minute"]),
        "second":          _int_col(passes["second"]),
        "location_x":      loc["location_x"],
        "location_y":      loc["location_y"],
        "end_location_x":  end_loc["end_location_x"],
        "end_location_y":  end_loc["end_location_y"],
        "length":          pd.to_numeric(_col(passes, "pass_length"),  errors="coerce"),
        "angle":           pd.to_numeric(_col(passes, "pass_angle"),   errors="coerce"),
        "height":          _col(passes, "pass_height"),
        "body_part":       _col(passes, "pass_body_part"),
        "outcome":         _col(passes, "pass_outcome"),
        "is_completed":    _col(passes, "pass_outcome").isna(),
        "pass_type":       _col(passes, "pass_type"),
        "technique":       _col(passes, "pass_technique"),
        "recipient_id":    _int_col(_col(passes, "pass_recipient_id")),
        "is_cross":        _bool_col(_col(passes, "pass_cross")),
        "is_switch":       _bool_col(_col(passes, "pass_switch")),
        "is_through_ball": _bool_col(_col(passes, "pass_through_ball")),
        "is_shot_assist":  _bool_col(_col(passes, "pass_shot_assist")),
        "is_goal_assist":  _bool_col(_col(passes, "pass_goal_assist")),
        "under_pressure":  _bool_col(_col(passes, "under_pressure")),
    })
    _drop_create(con, "fact_passes", out)


def build_fact_shots(con, df: pd.DataFrame) -> None:
    shots = df[df["type"] == "Shot"].copy().reset_index(drop=True)

    loc     = unpack_location(shots["location"])
    end_loc = unpack_location(_col(shots, "shot_end_location"), prefix="end_location")
    end_z   = end_loc.get("end_location_z", pd.Series([None] * len(shots)))

    out = pd.DataFrame({
        "event_id":       shots["id"],
        "match_id":       shots["match_id"],
        "player_id":      _int_col(_col(shots, "player_id")),
        "team_id":        _int_col(_col(shots, "team_id")),
        "period":         _int_col(shots["period"]),
        "minute":         _int_col(shots["minute"]),
        "second":         _int_col(shots["second"]),
        "location_x":     loc["location_x"],
        "location_y":     loc["location_y"],
        "end_location_x": end_loc["end_location_x"],
        "end_location_y": end_loc["end_location_y"],
        "end_location_z": end_z,
        "xg":             pd.to_numeric(_col(shots, "shot_statsbomb_xg"), errors="coerce"),
        "outcome":        _col(shots, "shot_outcome"),
        "is_goal":        _col(shots, "shot_outcome") == "Goal",
        "body_part":      _col(shots, "shot_body_part"),
        "technique":      _col(shots, "shot_technique"),
        "shot_type":      _col(shots, "shot_type"),
        "is_first_time":  _bool_col(_col(shots, "shot_first_time")),
        "key_pass_id":    _col(shots, "shot_key_pass_id"),
        "under_pressure": _bool_col(_col(shots, "under_pressure")),
    })
    _drop_create(con, "fact_shots", out)


def build_fact_carries(con, df: pd.DataFrame) -> None:
    carries = df[df["type"] == "Carry"].copy().reset_index(drop=True)

    loc     = unpack_location(carries["location"])
    end_loc = unpack_location(_col(carries, "carry_end_location"), prefix="end_location")

    out = pd.DataFrame({
        "event_id":       carries["id"],
        "match_id":       carries["match_id"],
        "player_id":      _int_col(_col(carries, "player_id")),
        "team_id":        _int_col(_col(carries, "team_id")),
        "period":         _int_col(carries["period"]),
        "minute":         _int_col(carries["minute"]),
        "second":         _int_col(carries["second"]),
        "location_x":     loc["location_x"],
        "location_y":     loc["location_y"],
        "end_location_x": end_loc["end_location_x"],
        "end_location_y": end_loc["end_location_y"],
        "duration":       pd.to_numeric(_col(carries, "duration"), errors="coerce"),
        "under_pressure": _bool_col(_col(carries, "under_pressure")),
    })
    _drop_create(con, "fact_carries", out)


def build_fact_lineups(con) -> None:
    df = _read_bronze("lineups")

    out = pd.DataFrame({
        "match_id":      df["match_id"],
        "team_id":       _int_col(_col(df, "team_id")),
        "team_name":     _col(df, "team_name"),
        "player_id":     _int_col(df["player_id"]),
        "player_name":   df["player_name"],
        "jersey_number": _int_col(_col(df, "jersey_number")),
        "position":      _col(df, "position"),
        "is_starter":    _col(df, "position").notna(),
    })
    _drop_create(con, "fact_lineups", out)


def build_bridge_shot_freeze_frame(con, df: pd.DataFrame) -> None:
    shots = df[df["type"] == "Shot"].copy().reset_index(drop=True)
    shots = shots[_col(shots, "shot_freeze_frame").notna()]

    rows = []
    for _, shot in shots.iterrows():
        frame = shot.get("shot_freeze_frame")
        if not isinstance(frame, (list, np.ndarray)):
            continue
        for player in frame:
            if not isinstance(player, dict):
                continue
            loc = player.get("location")
            # Fix: avoid truth value check on numpy array
            if loc is None:
                lx, ly = None, None
            else:
                loc = list(loc)
                lx = float(loc[0]) if len(loc) > 0 else None
                ly = float(loc[1]) if len(loc) > 1 else None
            pos = player.get("position", {})
            rows.append({
                "event_id":    shot["id"],
                "match_id":    shot["match_id"],
                "player_id":   player.get("player", {}).get("id"),
                "player_name": player.get("player", {}).get("name"),
                "position":    pos.get("name") if isinstance(pos, dict) else pos,
                "location_x":  lx,
                "location_y":  ly,
                "is_teammate": player.get("teammate", False),
            })

    if rows:
        out = pd.DataFrame(rows)
        _drop_create(con, "bridge_shot_freeze_frame", out)
    else:
        con.execute("DROP TABLE IF EXISTS bridge_shot_freeze_frame")
        con.execute("""
            CREATE TABLE bridge_shot_freeze_frame (
                event_id VARCHAR, match_id INTEGER, player_id INTEGER,
                player_name VARCHAR, position VARCHAR,
                location_x DOUBLE, location_y DOUBLE, is_teammate BOOLEAN
            )
        """)
        log.info("  bridge_shot_freeze_frame: 0 rows")


# ── Entry point ────────────────────────────────────────────────────────────────

def run() -> None:
    log.info("Building silver layer …")
    con = get_db_connection(read_only=False)
    try:
        log.info("dim tables …")
        build_dim_competition(con)
        build_dim_match(con)
        build_dim_team(con)
        build_dim_player(con)

        log.info("fact tables …")
        # Read events ONCE — used by all fact builders
        log.info("  reading all events into memory …")
        df_events = _read_bronze("events")
        # Ensure no duplicate columns after concat + normalise
        df_events = df_events.loc[:, ~df_events.columns.duplicated(keep="first")]

        build_fact_events(con, df_events)
        build_fact_passes(con, df_events)
        build_fact_shots(con, df_events)
        build_fact_carries(con, df_events)
        build_fact_lineups(con)
        build_bridge_shot_freeze_frame(con, df_events)

        log.info("Silver layer complete.")
    finally:
        con.close()


if __name__ == "__main__":
    run()