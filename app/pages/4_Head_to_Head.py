"""
Head to Head — compare any two teams across all metrics.
"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import get_competitions, get_team_stats, get_connection

st.set_page_config(page_title="Head to Head", page_icon="⚔️", layout="wide")
st.title("⚔️ Head to Head")

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("Select Competition")
comps = get_competitions()
comp_labels = (comps["competition_name"] + " — " + comps["season_name"]).tolist()
comp_idx = st.sidebar.selectbox("Competition", range(len(comp_labels)),
                                 format_func=lambda i: comp_labels[i])
selected_comp = comps.iloc[comp_idx]
comp_id = int(selected_comp["competition_id"])
season_id = int(selected_comp["season_id"])

# ── Load + aggregate ───────────────────────────────────────────────────────────
ts = get_team_stats(comp_id, season_id)

if ts.empty:
    st.warning("No data for this competition.")
    st.stop()

agg = ts.groupby("team_name").agg(
    matches      = ("match_id", "nunique"),
    total_xg     = ("total_xg", "sum"),
    total_shots  = ("total_shots", "sum"),
    total_goals  = ("goals", "sum"),
    avg_ppda     = ("ppda", "mean"),
    avg_pass_pct = ("pass_completion_pct", "mean"),
).reset_index()

agg["xg_per_game"]    = (agg["total_xg"] / agg["matches"]).round(3)
agg["shots_per_game"] = (agg["total_shots"] / agg["matches"]).round(1)
agg["goals_per_game"] = (agg["total_goals"] / agg["matches"]).round(2)
agg["avg_ppda"]       = agg["avg_ppda"].round(2)
agg["avg_pass_pct"]   = agg["avg_pass_pct"].round(1)

teams = sorted(agg["team_name"].tolist())

col1, col2 = st.columns(2)
with col1:
    team_a = st.selectbox("Team A", teams, index=0)
with col2:
    team_b = st.selectbox("Team B", teams,
                           index=min(1, len(teams)-1))

if team_a == team_b:
    st.warning("Select two different teams.")
    st.stop()

a = agg[agg["team_name"] == team_a].iloc[0]
b = agg[agg["team_name"] == team_b].iloc[0]

st.markdown("---")

# ── Summary metrics ────────────────────────────────────────────────────────────
st.subheader(f"{team_a}  vs  {team_b}")

metrics_display = [
    ("xG per Game",    "xg_per_game",    True),
    ("Shots per Game", "shots_per_game", True),
    ("Goals per Game", "goals_per_game", True),
    ("Avg PPDA",       "avg_ppda",       False),  # lower is better
    ("Pass %",         "avg_pass_pct",   True),
]

cols = st.columns(len(metrics_display))
for col, (label, field, higher_better) in zip(cols, metrics_display):
    val_a = a[field]
    val_b = b[field]
    if higher_better:
        delta = round(val_a - val_b, 3)
    else:
        delta = round(val_b - val_a, 3)  # for PPDA lower is better
    col.metric(
        label=f"{label}",
        value=f"{val_a}",
        delta=f"vs {val_b} ({team_b})",
        delta_color="normal" if delta >= 0 else "inverse"
    )

st.markdown("---")

# ── Radar comparison ───────────────────────────────────────────────────────────
st.subheader("Radar Comparison")

radar_metrics = ["xg_per_game", "shots_per_game", "goals_per_game", "avg_pass_pct"]
radar_labels  = ["xG/Game", "Shots/Game", "Goals/Game", "Pass %"]

# Normalise against all teams
norm_agg = agg.copy()
for m in radar_metrics:
    max_v = norm_agg[m].max()
    norm_agg[m] = (norm_agg[m] / max_v * 100).round(1) if max_v > 0 else 0

# PPDA: invert so higher = better press
if "avg_ppda" in norm_agg.columns:
    max_ppda = agg["avg_ppda"].max()
    norm_agg["ppda_inv"] = ((max_ppda - agg["avg_ppda"]) / max_ppda * 100).round(1)
    radar_metrics.append("ppda_inv")
    radar_labels.append("Press Intensity")

na = norm_agg[norm_agg["team_name"] == team_a].iloc[0]
nb = norm_agg[norm_agg["team_name"] == team_b].iloc[0]

vals_a = [na[m] for m in radar_metrics] + [na[radar_metrics[0]]]
vals_b = [nb[m] for m in radar_metrics] + [nb[radar_metrics[0]]]
theta  = radar_labels + [radar_labels[0]]

fig = go.Figure()
fig.add_trace(go.Scatterpolar(
    r=vals_a, theta=theta, fill="toself",
    name=team_a, line_color="#e74c3c", fillcolor="#e74c3c", opacity=0.3
))
fig.add_trace(go.Scatterpolar(
    r=vals_b, theta=theta, fill="toself",
    name=team_b, line_color="#3498db", fillcolor="#3498db", opacity=0.3
))
fig.update_layout(
    polar=dict(
        radialaxis=dict(visible=True, range=[0, 100],
                        tickfont=dict(color="white")),
        angularaxis=dict(tickfont=dict(color="white"))
    ),
    template="plotly_dark", height=500
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Match history between these two teams ──────────────────────────────────────
st.subheader("Matches Between These Teams")
con = get_connection()
h2h = con.execute("""
    SELECT match_date, home_team, home_score, away_score, away_team, match_week
    FROM dim_match
    WHERE competition_id = ? AND season_id = ?
      AND ((home_team = ? AND away_team = ?)
        OR (home_team = ? AND away_team = ?))
    ORDER BY match_date
""", [comp_id, season_id, team_a, team_b, team_b, team_a]).df()

if h2h.empty:
    st.info("These teams did not meet in this competition.")
else:
    st.dataframe(h2h, use_container_width=True)

st.markdown("---")

# ── xG trend: both teams across season ────────────────────────────────────────
st.subheader("xG Trend Across Season")

team_ts = ts[ts["team_name"].isin([team_a, team_b])].copy()
team_ts = team_ts.merge(
    con.execute("SELECT match_id, match_week, match_date FROM dim_match").df(),
    on="match_id", how="left"
).sort_values(["team_name", "match_week"])

if not team_ts.empty and "total_xg" in team_ts.columns:
    fig2 = px.line(
        team_ts, x="match_week", y="total_xg",
        color="team_name",
        markers=True,
        color_discrete_map={team_a: "#e74c3c", team_b: "#3498db"},
        labels={"match_week": "Match Week", "total_xg": "xG", "team_name": "Team"},
        template="plotly_dark"
    )
    fig2.update_layout(height=380)
    st.plotly_chart(fig2, use_container_width=True)