"""Match summary endpoint."""
from fastapi import APIRouter, Depends, HTTPException
import duckdb

from api.dependencies import get_db

router = APIRouter()


@router.get("/matches/{match_id}/summary")
def match_summary(
    match_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    """Return metadata + team stats for a single match."""
    # Match metadata
    meta = db.execute("""
        SELECT match_id, match_date, kick_off, home_team, away_team,
               home_score, away_score, stadium, stage, match_week
        FROM dim_match WHERE match_id = ?
    """, [match_id]).fetchone()

    if not meta:
        raise HTTPException(status_code=404, detail=f"Match {match_id} not found")

    meta_cols = ["match_id", "match_date", "kick_off", "home_team", "away_team",
                 "home_score", "away_score", "stadium", "stage", "match_week"]
    result = dict(zip(meta_cols, meta))
    if result["match_date"] is not None:
        result["match_date"] = str(result["match_date"])[:10]

    # Team stats
    stats_rows = db.execute("""
        SELECT team_id, team_name, total_shots, shots_on_target, goals,
               total_xg, total_passes, pass_completion_pct, total_carries, total_pressures
        FROM gold_team_stats WHERE match_id = ?
        ORDER BY team_name
    """, [match_id]).fetchall()

    stats_cols = ["team_id", "team_name", "total_shots", "shots_on_target", "goals",
                  "total_xg", "total_passes", "pass_completion_pct", "total_carries", "total_pressures"]
    result["team_stats"] = [dict(zip(stats_cols, r)) for r in stats_rows]

    # Teams (for UI dropdowns)
    teams = db.execute("""
        SELECT DISTINCT team_id, team AS team_name
        FROM fact_events
        WHERE match_id = ? AND team_id IS NOT NULL
        ORDER BY team_name
    """, [match_id]).fetchall()
    result["teams"] = [{"team_id": t[0], "team_name": t[1]} for t in teams]

    return result
