# StatsBomb Football Analytics Dashboard

A production-grade football analytics platform built on StatsBomb open data. Covers FIFA World Cup 2022 and Premier League 2015/16 — 444 matches, 1.5M+ events, 847 players.

Includes a custom xG model, xT (Expected Threat) model via Markov chain value iteration, and PPDA pressing intensity metrics — all built from first principles and compared against StatsBomb's professional benchmarks.

---

## Dashboard Pages

### 📋 Match Report
Select any match and get an instant post-match report:
- **xG Timeline** — cumulative expected goals over 90 minutes for both teams
- **Shot Map** — all shots with xG bubble sizing and outcome colour coding
- **Pass Network** — nodes sized by pass volume, edges weighted by connection frequency
- **Average Positions** — starters on pitch, substitutes shown separately
- **Pressure Heatmap** — where each team applied defensive pressure

### 📊 Team Overview
Season-level analysis across all teams:
- xG per game rankings
- PPDA pressing intensity table (Leicester 2015/16 insight built in)
- Pass completion % comparison
- xG vs actual goals — over/underperformance scatter

### 👤 Player Analysis
- xG leaders (total + per 90)
- xG over/underperformance (goals minus xG)
- xT-added leaders — passes vs carries split
- Key passers and chance creators
- Radar chart comparison — up to 3 players across 6 metrics

### ⚔️ Head to Head
Compare any two teams:
- Side-by-side metrics (xG, shots, goals, PPDA, pass %)
- Radar comparison chart
- Match history between the two teams
- xG trend across the season for both teams

---

## Analytical Models

### Custom xG Model (Logistic Regression)
Built from scratch and benchmarked against StatsBomb's professional model:

| Metric | Our Model | StatsBomb |
|--------|-----------|-----------|
| ROC-AUC | 0.762 | 0.798 |
| Log-loss | 0.272 | 0.255 |
| Brier Score | 0.077 | 0.071 |
| Total xG | 1,066.6 | 1,038.0 |
| Actual Goals | 1,066 | 1,066 |

Features: distance, angle, distance², body part, shot type, first touch, under pressure. Penalties excluded from training (fixed at 0.76 conversion rate).

### xT Model (Expected Threat — Markov Chain)
16×12 pitch grid (192 zones). Value iteration over 10 iterations converges to zone probabilities. xT-added assigned to every completed pass and carry.

Top result: Fàbregas (20.68 total xT), Özil (18.35), Payet (15.66 in 30 matches).

### PPDA (Passes Per Defensive Action)
Pressing intensity per team per match. Defensive zone = x > 48 (opponent's 60%).

Key finding: Leicester City PPDA = **8.69** — 3rd lowest press in the PL top 6. They won the title by *not* pressing while Tottenham (5.93) and Liverpool (6.26) pressed aggressively.

---

## Architecture

```
StatsBomb JSON
      │
      ▼
Bronze Layer (Parquet)
  Raw JSON → Parquet, idempotent, skips existing files
      │
      ▼
Silver Layer (DuckDB star schema)
  4 dimension tables + 5 fact tables + 1 bridge table
  Handles flat vs dot-notation schema differences across competitions
      │
      ▼
Gold Layer (DuckDB aggregations)
  Pre-aggregated chart datasets + model outputs
  gold_xg_timeline, gold_shot_map, gold_pass_network_*
  gold_formation_positions, gold_team_stats
  gold_xt_grid, gold_xt_player
  gold_ppda_match, gold_ppda_team
      │
      ▼
Streamlit App (4 pages)
  mplsoccer charts (Match Report)
  Plotly interactive charts (Overview, Players, H2H)
```

## Schema

```
dim_competition    dim_match    dim_team    dim_player
      │                │            │            │
      └────────────────┴────────────┴────────────┘
                       │
         ┌─────────────┼──────────────┐
         │             │              │
   fact_events   fact_passes    fact_shots
   fact_carries  fact_lineups
                 bridge_shot_freeze_frame
```

---

## Stack

| Layer | Technology |
|-------|-----------|
| Data source | StatsBomb open-data (statsbombpy) |
| Storage | Parquet (bronze), DuckDB (silver/gold) |
| Pipeline | Python, pandas (vectorised) |
| Models | scikit-learn (xG), NumPy (xT), custom SQL (PPDA) |
| Dashboard | Streamlit, Plotly, mplsoccer, matplotlib |

---

## Running Locally

```bash
# Setup
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Add competitions to config/competitions.yaml, then run pipeline
python scripts/run_pipeline.py --all

# Train analytical models
python scripts/xg_model.py          # trains + saves models/xg_model.pkl
python scripts/apply_xg_model.py    # writes xg_model column to fact_shots
python scripts/xt_model.py          # builds xT grid + gold_xt_player table
python scripts/ppda.py              # builds gold_ppda_match + gold_ppda_team

# Launch dashboard
streamlit run app/app.py
# Opens at http://localhost:8501
```

## Adding a New Competition

Edit `config/competitions.yaml`:

```yaml
- competition_id: 11
  season_id: 42
  name: "La Liga 2020/21"
  enabled: true
```

Then run `python scripts/run_pipeline.py --all`. Existing data is skipped automatically.

---

## Data

StatsBomb open data is not included in this repo. It is downloaded automatically by the pipeline from the [StatsBomb open-data GitHub repository](https://github.com/statsbomb/open-data).

---

## Project Context

Built as a portfolio project during a Master's in Sports Analytics at Universidad Europea de Madrid. The goal was to apply production data engineering patterns (medallion architecture, dimensional modelling, config-driven pipelines) to football analytics — and build the analytical models (xG, xT, PPDA) from first principles rather than relying on pre-computed values.

The custom xG model achieves 95% of StatsBomb's professional model performance using 8 features vs their 50+. The xT model via Markov chain value iteration reproduces Karun Singh's 2018 paper results on this dataset.
