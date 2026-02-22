"""
Downloads StatsBomb open data from GitHub raw URLs.
Config-driven: reads enabled competitions from config/competitions.yaml.
"""
import json
from pathlib import Path
from typing import Any
import time

import ssl
import requests
import yaml

from config.settings import PROJECT_ROOT, BRONZE_PATH
from pipeline.logger import get_logger

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = get_logger(__name__)

_BASE_URL = (
    "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
)
_COMP_FILE = PROJECT_ROOT / "config" / "competitions.yaml"

# Fall back to worktree-local config if main repo config doesn't exist
if not _COMP_FILE.exists():
    _COMP_FILE = Path(__file__).parent.parent / "config" / "competitions.yaml"

_RAW_DIR = PROJECT_ROOT / "data" / "statsbomb_raw"


def _load_competitions_config() -> list[dict]:
    with open(_COMP_FILE) as f:
        cfg = yaml.safe_load(f)
    return [c for c in cfg.get("competitions", []) if c.get("enabled")]


def _fetch_json(url: str, retries: int = 5) -> dict:
   
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30, verify=False)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.SSLError as e:
            log.warning(f"SSL error attempt {attempt}/{retries}: {url}")
            if attempt < retries:
                time.sleep(3 * attempt)
            else:
                raise
        except requests.exceptions.RequestException as e:
            log.warning(f"Request error attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(3 * attempt)
            else:
                raise


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def download_competitions() -> None:
    """Download the master competitions list."""
    url = f"{_BASE_URL}/competitions.json"
    log.info("Downloading competitions list …")
    data = _fetch_json(url)
    dest = _RAW_DIR / "competitions" / "competitions.json"
    _save_json(dest, data)
    log.info(f"Saved {len(data)} competitions → {dest}")


def download_matches(competition_id: int, season_id: int) -> None:
    """Download matches for a single competition/season."""
    url = f"{_BASE_URL}/matches/{competition_id}/{season_id}.json"
    log.info(f"Downloading matches {competition_id}/{season_id} …")
    data = _fetch_json(url)
    dest = _RAW_DIR / "matches" / str(competition_id) / f"{season_id}.json"
    _save_json(dest, data)
    log.info(f"Saved {len(data)} matches → {dest}")


def download_events_and_lineups(match_ids: list[int]) -> None:
    """Download events + lineups for a list of match IDs."""
    for mid in match_ids:
        for kind in ("events", "lineups"):
            url = f"{_BASE_URL}/{kind}/{mid}.json"
            dest = _RAW_DIR / kind / f"{mid}.json"
            if dest.exists():
                log.debug(f"Skip (exists): {dest.name}")
                continue
            log.info(f"Downloading {kind}/{mid}.json …")
            data = _fetch_json(url)
            _save_json(dest, data)


def run() -> None:
    """Entry point: download all enabled competitions."""
    competitions = _load_competitions_config()
    log.info(f"Found {len(competitions)} enabled competition(s) in config")

    download_competitions()

    # Load competition list to get match IDs
    comp_path = _RAW_DIR / "competitions" / "competitions.json"
    all_comps = json.loads(comp_path.read_text())

    for comp in competitions:
        cid = comp["competition_id"]
        sid = comp["season_id"]
        log.info(f"Processing: {comp['name']} ({cid}/{sid})")

        download_matches(cid, sid)

        match_file = _RAW_DIR / "matches" / str(cid) / f"{sid}.json"
        matches = json.loads(match_file.read_text())
        match_ids = [m["match_id"] for m in matches]
        log.info(f"Found {len(match_ids)} matches")

        download_events_and_lineups(match_ids)

    log.info("Download complete.")


if __name__ == "__main__":
    run()
