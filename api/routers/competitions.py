"""Competitions and matches listing endpoints."""
from fastapi import APIRouter, Depends, HTTPException
import duckdb

from api.dependencies import get_db

router = APIRouter()


@router.get("/competitions")
def list_competitions(db: duckdb.DuckDBPyConnection = Depends(get_db)):
    """List all available competitions."""
    rows = db.execute("""
        SELECT DISTINCT
            competition_id,
            season_id,
            competition_name,
            season_name,
            country_name
        FROM dim_competition
        ORDER BY competition_name, season_name
    """).fetchall()
    cols = ["competition_id", "season_id", "competition_name", "season_name", "country_name"]
    return [dict(zip(cols, r)) for r in rows]


@router.get("/competitions/{competition_id}/{season_id}/matches")
def list_matches(
    competition_id: int,
    season_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    """List all matches for a given competition and season."""
    rows = db.execute("""
        SELECT
            match_id,
            match_date,
            kick_off,
            home_team,
            away_team,
            home_score,
            away_score,
            stadium,
            stage,
            match_week
        FROM dim_match
        WHERE competition_id = ? AND season_id = ?
        ORDER BY match_date, kick_off
    """, [competition_id, season_id]).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No matches found")

    cols = ["match_id", "match_date", "kick_off", "home_team", "away_team",
            "home_score", "away_score", "stadium", "stage", "match_week"]
    result = []
    for r in rows:
        d = dict(zip(cols, r))
        # Convert date to string for JSON serialisation
        if d["match_date"] is not None:
            d["match_date"] = str(d["match_date"])[:10]
        result.append(d)
    return result
