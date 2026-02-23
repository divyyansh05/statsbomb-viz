"""
Team Overview — season xG performance, PPDA rankings, pass completion trends.
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import get_competitions, get_matches, get_team_stats, get_connection

st.set_page_config(page_title="Team Overview", page_icon="📊", layout="wide")
st.title("📊 Team Overview")

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("Select Competition")
comps = get_competitions()
comp_labels = (comps["competition_name"] + " — " + comps["season_name"]).tolist()
comp_idx = st.sidebar.selectbox("Competition", range(len(comp_labels)),
                                 format_func=lambda i: comp_labels[i])
selected_comp = comps.iloc[comp_idx]
comp_id = int(selected_comp["competition_id"])
season_id = int(selected_comp["season_id"])

# ── Load data ──────────────────────────────────────────────────────────────────
ts = get_team_stats(comp_id, season_id)

if ts.empty:
    st.warning("No team stats found for this competition.")
    st.stop()

# Aggregate per team across season
agg = ts.groupby("team_name").agg(
    matches       = ("match_id", "nunique"),
    total_xg      = ("total_xg", "sum"),
    avg_ppda      = ("ppda", "mean"),
    avg_pass_pct  = ("pass_completion_pct", "mean"),
    total_shots   = ("total_shots", "sum"),
    total_goals   = ("goals", "sum"),
).reset_index()

agg["xg_per_game"]    = (agg["total_xg"] / agg["matches"]).round(3)
agg["goals_per_game"] = (agg["total_goals"] / agg["matches"]).round(2)
agg["avg_ppda"]       = agg["avg_ppda"].round(2)
agg["avg_pass_pct"]   = agg["avg_pass_pct"].round(1)
agg = agg.sort_values("xg_per_game", ascending=False)

# ── Season xG table ────────────────────────────────────────────────────────────
st.subheader("Season xG Performance")

fig = px.bar(
    agg.head(20),
    x="team_name", y="xg_per_game",
    color="xg_per_game",
    color_continuous_scale="Reds",
    labels={"team_name": "Team", "xg_per_game": "xG per Game"},
    template="plotly_dark"
)
fig.update_layout(
    xaxis_tickangle=-45,
    coloraxis_showscale=False,
    height=420,
    margin=dict(t=30, b=120)
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── PPDA Pressing Rankings ─────────────────────────────────────────────────────
st.subheader("Pressing Intensity (PPDA)")
st.caption("Lower PPDA = more aggressive press")

ppda_df = agg[agg["avg_ppda"].notna()].sort_values("avg_ppda")

fig2 = px.bar(
    ppda_df.head(20),
    x="team_name", y="avg_ppda",
    color="avg_ppda",
    color_continuous_scale="Blues_r",
    labels={"team_name": "Team", "avg_ppda": "Avg PPDA"},
    template="plotly_dark"
)
fig2.update_layout(
    xaxis_tickangle=-45,
    coloraxis_showscale=False,
    height=420,
    margin=dict(t=30, b=120)
)
st.plotly_chart(fig2, use_container_width=True)

# Leicester callout
leicester = agg[agg["team_name"] == "Leicester City"]
if not leicester.empty:
    ppda_val = leicester["avg_ppda"].values[0]
    rank = ppda_df.reset_index(drop=True)
    rank_pos = rank[rank["team_name"] == "Leicester City"].index[0] + 1
    st.info(f"📌 **Leicester City PPDA: {ppda_val:.2f}** — ranked #{rank_pos} "
            f"out of {len(ppda_df)} teams. They won the title by *not* pressing.")

st.markdown("---")

# ── Pass Completion ────────────────────────────────────────────────────────────
st.subheader("Pass Completion %")

pass_df = agg[agg["avg_pass_pct"].notna()].sort_values("avg_pass_pct", ascending=False)

fig3 = px.bar(
    pass_df.head(20),
    x="team_name", y="avg_pass_pct",
    color="avg_pass_pct",
    color_continuous_scale="Greens",
    labels={"team_name": "Team", "avg_pass_pct": "Pass Completion %"},
    template="plotly_dark"
)
fig3.update_layout(
    xaxis_tickangle=-45,
    coloraxis_showscale=False,
    height=420,
    margin=dict(t=30, b=120)
)
st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")

# ── xG vs Goals Scatter ────────────────────────────────────────────────────────
st.subheader("xG vs Actual Goals — Over/Underperformance")

fig4 = px.scatter(
    agg,
    x="xg_per_game", y="goals_per_game",
    text="team_name",
    template="plotly_dark",
    labels={"xg_per_game": "xG per Game", "goals_per_game": "Goals per Game"},
    color="goals_per_game",
    color_continuous_scale="RdYlGn",
    size="matches",
)
# Diagonal line = perfect xG conversion
max_val = max(agg["xg_per_game"].max(), agg["goals_per_game"].max()) + 0.1
fig4.add_shape(
    type="line", x0=0, y0=0, x1=max_val, y1=max_val,
    line=dict(color="white", dash="dash", width=1)
)
fig4.add_annotation(
    x=max_val * 0.8, y=max_val * 0.85,
    text="Perfect conversion", showarrow=False,
    font=dict(color="white", size=10)
)
fig4.update_traces(textposition="top center", textfont_size=9)
fig4.update_layout(height=500, coloraxis_showscale=False)
st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ── Raw table ──────────────────────────────────────────────────────────────────
with st.expander("Full Season Stats Table"):
    st.dataframe(
        agg[["team_name","matches","xg_per_game","goals_per_game",
             "avg_ppda","avg_pass_pct","total_shots"]].round(3),
        use_container_width=True
    )