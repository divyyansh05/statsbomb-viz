"""
Database utilities for Streamlit app.
Each function opens and closes its own DuckDB connection.
Using @st.cache_data for query results, not connection sharing.
"""
import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "gold" / "statsbomb.duckdb"


def _conn():
    """Open a fresh read-only connection. Always close after use."""
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_resource
def get_connection():
    """Shared connection for ad-hoc queries in page files."""
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data
def get_competitions() -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT DISTINCT c.competition_id, c.season_id,
               c.competition_name, c.season_name, c.country_name
        FROM dim_competition c
        INNER JOIN dim_match m
            ON c.competition_id = m.competition_id
            AND c.season_id = m.season_id
        ORDER BY c.competition_name, c.season_name
    """).df()
    con.close()
    return df


@st.cache_data
def get_matches(competition_id: int, season_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT match_id, match_date, match_week,
               home_team, away_team, home_score, away_score
        FROM dim_match
        WHERE competition_id = ? AND season_id = ?
        ORDER BY match_date, match_week
    """, [competition_id, season_id]).df()
    con.close()
    return df


@st.cache_data
def get_match_teams(match_id: int) -> list:
    con = _conn()
    df = con.execute("""
        SELECT DISTINCT team FROM fact_events
        WHERE match_id = ? AND team IS NOT NULL
    """, [match_id]).df()
    con.close()
    return df["team"].tolist()


@st.cache_data
def get_team_id(match_id: int, team_name: str) -> int:
    """Resolve team_name to team_id directly from gold_formation_positions."""
    con = _conn()
    result = con.execute("""
        SELECT DISTINCT team_id 
        FROM gold_formation_positions
        WHERE match_id = ? AND team_name = ?
        LIMIT 1
    """, [match_id, team_name]).df()
    con.close()
    if result.empty:
        return None
    val = result["team_id"].iloc[0]
    if val is None or (hasattr(val, '__class__') and val != val):
        return None
    return int(val)


@st.cache_data
def get_team_stats(competition_id: int, season_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT ts.*
        FROM gold_team_stats ts
        JOIN dim_match m ON ts.match_id = m.match_id
        WHERE m.competition_id = ? AND m.season_id = ?
    """, [competition_id, season_id]).df()
    con.close()
    return df


@st.cache_data
def get_xg_timeline(match_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT * FROM gold_xg_timeline
        WHERE match_id = ?
        ORDER BY period, minute, second
    """, [match_id]).df()
    con.close()
    return df


@st.cache_data
def get_shot_map(match_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT * FROM gold_shot_map
        WHERE match_id = ?
    """, [match_id]).df()
    con.close()
    return df


@st.cache_data
def get_pass_network(match_id: int, team_id: int) -> tuple:
    con = _conn()
    nodes = con.execute("""
        SELECT * FROM gold_pass_network_nodes
        WHERE match_id = ? AND team_id = ?
    """, [match_id, team_id]).df()
    edges = con.execute("""
        SELECT * FROM gold_pass_network_edges
        WHERE match_id = ? AND team_id = ?
    """, [match_id, team_id]).df()
    con.close()
    return nodes, edges


@st.cache_data
def get_formation_starters(match_id: int, team_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        WITH ranked AS (
            SELECT l.player_id, l.player_name, l.jersey_number, l.position,
                   f.avg_x, f.avg_y, f.touch_count,
                   ROW_NUMBER() OVER (ORDER BY f.touch_count DESC) as rn
            FROM fact_lineups l
            INNER JOIN gold_formation_positions f
                ON l.match_id = f.match_id
                AND l.player_id = f.player_id
                AND l.team_name = f.team_name
            WHERE l.match_id = ?
              AND f.team_id = ?
              AND f.avg_x IS NOT NULL
        )
        SELECT player_id, player_name, jersey_number, position,
               avg_x, avg_y, touch_count
        FROM ranked
        WHERE rn <= 11
    """, [match_id, team_id]).df()
    con.close()
    return df


@st.cache_data
def get_formation_subs(match_id: int, team_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        WITH ranked AS (
            SELECT l.player_id, l.player_name, l.jersey_number, l.position,
                   f.avg_x, f.avg_y, f.touch_count,
                   ROW_NUMBER() OVER (ORDER BY f.touch_count DESC) as rn
            FROM fact_lineups l
            INNER JOIN gold_formation_positions f
                ON l.match_id = f.match_id
                AND l.player_id = f.player_id
                AND l.team_name = f.team_name
            WHERE l.match_id = ?
              AND f.team_id = ?
              AND f.avg_x IS NOT NULL
        )
        SELECT player_id, player_name, jersey_number, position,
               avg_x, avg_y, touch_count
        FROM ranked
        WHERE rn > 11
    """, [match_id, team_id]).df()
    con.close()
    return df




@st.cache_data
def get_player_xt(competition_id: int, season_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT DISTINCT
               p.player_id, p.player_name, p.total_xt_added,
               p.xt_per_90, p.actions_count, p.matches_played,
               p.xt_passes, p.xt_carries
        FROM gold_xt_player p
        JOIN fact_events e ON p.player_id = e.player_id
        JOIN dim_match m ON e.match_id = m.match_id
        WHERE m.competition_id = ? AND m.season_id = ?
    """, [competition_id, season_id]).df()
    con.close()
    return df


@st.cache_data
def get_ppda_table() -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT team, avg_ppda, matches
        FROM gold_ppda_team
        ORDER BY avg_ppda ASC
    """).df()
    con.close()
    return df


@st.cache_data
def get_pressure_events(match_id: int) -> pd.DataFrame:
    con = _conn()
    df = con.execute("""
        SELECT location_x, location_y, team
        FROM fact_events
        WHERE match_id = ? AND type = 'Pressure'
          AND location_x IS NOT NULL
    """, [match_id]).df()
    con.close()
    return df