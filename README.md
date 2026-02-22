# StatsBomb Football Analytics Dashboard

Post-match analytics dashboard built on StatsBomb open data. Covers FIFA World Cup 2022 and Premier League 2015/16 — 444 matches, 1.5M+ events.

## What it does

Select any match from the dropdown and get an instant post-match report:

- **Formation map** — average player positions derived from actual touch locations
- **Pass network** — nodes sized by pass volume, edges weighted by connection frequency  
- **Shot map** — all shots with xG bubble sizing, outcome colour coding
- **xG timeline** — cumulative expected goals over 90 minutes for both teams
- **Pressure heatmap** — where each team applied defensive pressure

## Architecture
```
StatsBomb JSON → Bronze (Parquet) → Silver (DuckDB star schema) → Gold (aggregations) → FastAPI → Dashboard
```

**Bronze layer** — Raw JSON converted to Parquet. Idempotent, skips existing files.

**Silver layer** — Star schema in DuckDB. 4 dimension tables, 5 fact tables, 1 bridge table. Handles schema differences between competitions (StatsBomb uses flat columns for some datasets, dot-notation for others).

**Gold layer** — Pre-aggregated chart datasets. One table per chart type, queried directly by API endpoints.

**API** — FastAPI. Chart endpoints render mplsoccer figures server-side, return base64 PNG. Zero frontend build step required.

## Schema
```
dim_competition    dim_match    dim_team    dim_player
      │                │            │            │
      └────────────────┴────────────┴────────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
   fact_events   fact_passes   fact_shots
                              fact_carries
                              fact_lineups
                        bridge_shot_freeze_frame
```

## Stack

| Layer | Technology |
|-------|-----------|
| Data source | StatsBomb open-data (statsbombpy) |
| Storage | Parquet (bronze), DuckDB (silver/gold) |
| Pipeline | Python, pandas (vectorised) |
| API | FastAPI, uvicorn |
| Charts | mplsoccer, matplotlib |
| Frontend | HTML, CSS Grid, vanilla JS |

## Running locally
```bash
# Setup
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Add competitions to config/competitions.yaml, then:
python scripts/run_pipeline.py --all

# Start API
uvicorn api.main:app --reload

# Open dashboard
open frontend/index.html
```

## Adding a new competition

Edit `config/competitions.yaml`:
```yaml
- competition_id: 11
  season_id: 42
  name: "La Liga 2020/21"
  enabled: true
```

Then run `python scripts/run_pipeline.py --all`. Existing data is skipped.

## Data

StatsBomb open data is not included in this repo. It is downloaded automatically by the pipeline from the [StatsBomb open-data GitHub repository](https://github.com/statsbomb/open-data).

## Project context

Built as a portfolio project during a Master's in Sports Analytics at Universidad Europea de Madrid. The goal was to apply production data engineering patterns (medallion architecture, dimensional modelling, config-driven pipelines) to football analytics data.
