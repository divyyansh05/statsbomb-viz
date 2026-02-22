"""
Bronze layer: converts raw StatsBomb JSON files to Parquet.
Idempotent — skips files that already exist unless forced.
"""
import json
from pathlib import Path

import pandas as pd

from config.settings import PROJECT_ROOT, BRONZE_PATH
from pipeline.logger import get_logger

log = get_logger(__name__)

_RAW_DIR = PROJECT_ROOT / "data" / "statsbomb_raw"


def _json_to_df(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding="utf-8"))
    return pd.json_normalize(data)


def ingest_competitions(force: bool = False) -> None:
    src = _RAW_DIR / "competitions" / "competitions.json"
    dst = BRONZE_PATH / "competitions" / "competitions.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not force:
        log.info(f"Skip (exists): {dst.name}")
        return
    df = _json_to_df(src)
    df.to_parquet(dst, index=False)
    log.info(f"competitions → {len(df)} rows → {dst}")


def ingest_matches(competition_id: int, season_id: int, force: bool = False) -> None:
    src = _RAW_DIR / "matches" / str(competition_id) / f"{season_id}.json"
    dst = BRONZE_PATH / "matches" / f"{competition_id}_{season_id}.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not force:
        log.info(f"Skip (exists): {dst.name}")
        return
    df = _json_to_df(src)
    df.to_parquet(dst, index=False)
    log.info(f"matches {competition_id}/{season_id} → {len(df)} rows → {dst}")


def ingest_events(match_id: int, force: bool = False) -> None:
    src = _RAW_DIR / "events" / f"{match_id}.json"
    dst = BRONZE_PATH / "events" / f"{match_id}.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not force:
        log.debug(f"Skip (exists): events/{match_id}.parquet")
        return
    df = _json_to_df(src)
    df["match_id"] = match_id 
    df.to_parquet(dst, index=False)
    log.info(f"events/{match_id} → {len(df)} rows")


def ingest_lineups(match_id: int, force: bool = False) -> None:
    src = _RAW_DIR / "lineups" / f"{match_id}.json"
    dst = BRONZE_PATH / "lineups" / f"{match_id}.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not force:
        log.debug(f"Skip (exists): lineups/{match_id}.parquet")
        return
    data = json.loads(src.read_text(encoding="utf-8"))
    rows = []
    for team in data:
        for player in team.get("lineup", []):
            row = {
                "match_id": match_id,
                "team_id": team.get("team_id"),
                "team_name": team.get("team_name"),
                "player_id": player.get("player_id"),
                "player_name": player.get("player_name"),
                "jersey_number": player.get("jersey_number"),
                "country": player.get("country", {}).get("name"),
            }
            # positions list
            positions = player.get("positions", [])
            for pos in positions:
                rows.append({**row, "position_id": pos.get("position_id"),
                             "position": pos.get("position"),
                             "from_time": pos.get("from"),
                             "to_time": pos.get("to"),
                             "from_period": pos.get("from_period"),
                             "to_period": pos.get("to_period"),
                             "start_reason": pos.get("start_reason"),
                             "end_reason": pos.get("end_reason")})
            if not positions:
                rows.append(row)
    df = pd.DataFrame(rows)
    df.to_parquet(dst, index=False)
    log.info(f"lineups/{match_id} → {len(df)} rows")


def run(force: bool = False) -> None:
    """Ingest all raw JSON into bronze Parquet."""
    import yaml
    from config.settings import PROJECT_ROOT as _PR

    cfg_path = Path(__file__).parent.parent / "config" / "competitions.yaml"
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f)
    competitions = [c for c in cfg.get("competitions", []) if c.get("enabled")]

    ingest_competitions(force=force)

    for comp in competitions:
        cid = comp["competition_id"]
        sid = comp["season_id"]
        ingest_matches(cid, sid, force=force)

        match_file = _RAW_DIR / "matches" / str(cid) / f"{sid}.json"
        matches = json.loads(match_file.read_text())
        match_ids = [m["match_id"] for m in matches]

        for mid in match_ids:
            ingest_events(mid, force=force)
            ingest_lineups(mid, force=force)

    log.info("Bronze ingestion complete.")


if __name__ == "__main__":
    run()
