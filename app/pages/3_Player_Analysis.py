"""
Player Analysis — xG, xT-added, key passes, radar charts.
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import get_competitions, get_connection

st.set_page_config(page_title="Player Analysis", page_icon="👤", layout="wide")
st.title("👤 Player Analysis")

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("Select Competition")
comps = get_competitions()
comp_labels = (comps["competition_name"] + " — " + comps["season_name"]).tolist()
comp_idx = st.sidebar.selectbox("Competition", range(len(comp_labels)),
                                 format_func=lambda i: comp_labels[i])
selected_comp = comps.iloc[comp_idx]
comp_id = int(selected_comp["competition_id"])
season_id = int(selected_comp["season_id"])

min_matches = st.sidebar.slider("Min matches played", 1, 20, 5)

# ── Load data ──────────────────────────────────────────────────────────────────
con = get_connection()

@st.cache_data
def load_player_stats(comp_id, season_id, min_matches):
    con = get_connection()

    shots = con.execute("""
        SELECT s.player_id, pl.player_name,
               COUNT(*) as shots,
               SUM(s.xg) as total_xg,
               SUM(s.is_goal::int) as goals,
               COUNT(DISTINCT s.match_id) as matches
        FROM fact_shots s
        JOIN dim_player pl ON s.player_id = pl.player_id
        JOIN dim_match m ON s.match_id = m.match_id
        WHERE m.competition_id = ? AND m.season_id = ?
        GROUP BY s.player_id, pl.player_name
        HAVING COUNT(DISTINCT s.match_id) >= ?
    """, [comp_id, season_id, min_matches]).df()

    xt = con.execute("""
        SELECT p.player_id, p.player_name,
               p.total_xt_added, p.xt_per_90,
               p.xt_passes, p.xt_carries,
               p.matches_played, p.actions_count
        FROM gold_xt_player p
        WHERE p.matches_played >= ?
    """, [min_matches]).df()

    passes = con.execute("""
        SELECT p.player_id, pl.player_name,
               COUNT(*) as total_passes,
               SUM(p.is_completed::int) as completed_passes,
               SUM(p.is_shot_assist::int) as shot_assists,
               SUM(p.is_goal_assist::int) as goal_assists,
               COUNT(DISTINCT p.match_id) as matches
        FROM fact_passes p
        JOIN dim_player pl ON p.player_id = pl.player_id
        JOIN dim_match m ON p.match_id = m.match_id
        WHERE m.competition_id = ? AND m.season_id = ?
        GROUP BY p.player_id, pl.player_name
        HAVING COUNT(DISTINCT p.match_id) >= ?
    """, [comp_id, season_id, min_matches]).df()

    return shots, xt, passes

shots_df, xt_df, passes_df = load_player_stats(comp_id, season_id, min_matches)

if shots_df.empty:
    st.warning("No player data for this competition.")
    st.stop()

shots_df["xg_per_90"] = (shots_df["total_xg"] / shots_df["matches"]).round(3)
shots_df["xg_overperformance"] = (shots_df["goals"] - shots_df["total_xg"]).round(3)
passes_df["key_pass_pct"] = (passes_df["shot_assists"] / passes_df["total_passes"] * 100).round(2)

# ── Tab layout ─────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["⚽ xG Leaders", "🔥 xT Leaders", "🎯 Key Passers", "📡 Radar"])

# ── Tab 1: xG Leaders ─────────────────────────────────────────────────────────
with tab1:
    st.subheader("Top xG Producers")

    col1, col2 = st.columns(2)

    with col1:
        top_xg = shots_df.sort_values("total_xg", ascending=False).head(15)
        fig = px.bar(
            top_xg, x="total_xg", y="player_name",
            orientation="h", color="total_xg",
            color_continuous_scale="Reds",
            labels={"total_xg": "Total xG", "player_name": ""},
            template="plotly_dark",
            title="Total xG (season)"
        )
        fig.update_layout(coloraxis_showscale=False, height=480,
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        top_xg90 = shots_df.sort_values("xg_per_90", ascending=False).head(15)
        fig2 = px.bar(
            top_xg90, x="xg_per_90", y="player_name",
            orientation="h", color="xg_per_90",
            color_continuous_scale="Oranges",
            labels={"xg_per_90": "xG per 90", "player_name": ""},
            template="plotly_dark",
            title="xG per 90"
        )
        fig2.update_layout(coloraxis_showscale=False, height=480,
                           yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("xG Over/Underperformance")
    st.caption("Positive = scoring more than xG predicts. Negative = underperforming.")
    overperf = shots_df.sort_values("xg_overperformance", ascending=False).head(20)
    fig3 = px.bar(
        overperf, x="player_name", y="xg_overperformance",
        color="xg_overperformance",
        color_continuous_scale="RdYlGn",
        labels={"xg_overperformance": "Goals - xG", "player_name": ""},
        template="plotly_dark"
    )
    fig3.update_layout(xaxis_tickangle=-45, coloraxis_showscale=False,
                       height=380, margin=dict(b=120))
    fig3.add_hline(y=0, line_color="white", line_dash="dash", line_width=1)
    st.plotly_chart(fig3, use_container_width=True)

# ── Tab 2: xT Leaders ─────────────────────────────────────────────────────────
with tab2:
    st.subheader("Expected Threat Leaders")
    st.caption("xT measures how much a player moves the ball into dangerous areas via passes and carries.")

    if xt_df.empty:
        st.info("No xT data available.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            top_xt = xt_df.sort_values("total_xt_added", ascending=False).head(15)
            fig = px.bar(
                top_xt, x="total_xt_added", y="player_name",
                orientation="h", color="total_xt_added",
                color_continuous_scale="Purples",
                labels={"total_xt_added": "Total xT Added", "player_name": ""},
                template="plotly_dark",
                title="Total xT Added (season)"
            )
            fig.update_layout(coloraxis_showscale=False, height=480,
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            top_xt90 = xt_df.sort_values("xt_per_90", ascending=False).head(15)
            fig2 = px.bar(
                top_xt90, x="xt_per_90", y="player_name",
                orientation="h", color="xt_per_90",
                color_continuous_scale="Blues",
                labels={"xt_per_90": "xT per 90", "player_name": ""},
                template="plotly_dark",
                title="xT per 90"
            )
            fig2.update_layout(coloraxis_showscale=False, height=480,
                               yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("xT Split: Passes vs Carries")
        top20 = xt_df.sort_values("total_xt_added", ascending=False).head(20)
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            y=top20["player_name"], x=top20["xt_passes"],
            name="Via Passes", orientation="h", marker_color="#3498db"
        ))
        fig3.add_trace(go.Bar(
            y=top20["player_name"], x=top20["xt_carries"],
            name="Via Carries", orientation="h", marker_color="#e74c3c"
        ))
        fig3.update_layout(
            barmode="stack", template="plotly_dark",
            height=500, yaxis=dict(autorange="reversed"),
            legend=dict(orientation="h", y=1.05)
        )
        st.plotly_chart(fig3, use_container_width=True)

# ── Tab 3: Key Passers ────────────────────────────────────────────────────────
with tab3:
    st.subheader("Key Passers & Chance Creators")

    col1, col2 = st.columns(2)

    with col1:
        top_assists = passes_df.sort_values("shot_assists", ascending=False).head(15)
        fig = px.bar(
            top_assists, x="shot_assists", y="player_name",
            orientation="h", color="shot_assists",
            color_continuous_scale="Greens",
            labels={"shot_assists": "Shot Assists (Key Passes)", "player_name": ""},
            template="plotly_dark",
            title="Shot Assists (Key Passes)"
        )
        fig.update_layout(coloraxis_showscale=False, height=480,
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        top_ga = passes_df.sort_values("goal_assists", ascending=False).head(15)
        fig2 = px.bar(
            top_ga, x="goal_assists", y="player_name",
            orientation="h", color="goal_assists",
            color_continuous_scale="YlOrRd",
            labels={"goal_assists": "Goal Assists", "player_name": ""},
            template="plotly_dark",
            title="Goal Assists"
        )
        fig2.update_layout(coloraxis_showscale=False, height=480,
                           yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig2, use_container_width=True)

# ── Tab 4: Radar Chart ────────────────────────────────────────────────────────
with tab4:
    st.subheader("Player Radar Comparison")
    st.caption("Compare up to 3 players across 6 metrics. Normalised 0–100.")

    # Merge all stats
    merged = shots_df[["player_name","total_xg","xg_per_90","goals","matches"]].merge(
        passes_df[["player_name","shot_assists","goal_assists","key_pass_pct"]],
        on="player_name", how="outer"
    ).merge(
        xt_df[["player_name","total_xt_added","xt_per_90"]],
        on="player_name", how="outer"
    ).fillna(0)

    all_players = sorted(merged["player_name"].unique().tolist())
    selected_players = st.multiselect(
        "Select players (2–3)", all_players,
        default=all_players[:2] if len(all_players) >= 2 else all_players
    )

    if len(selected_players) < 2:
        st.info("Select at least 2 players to compare.")
    else:
        metrics = ["total_xg", "xg_per_90", "shot_assists",
                   "goal_assists", "total_xt_added", "xt_per_90"]
        labels  = ["Total xG", "xG/90", "Shot Assists",
                   "Goal Assists", "Total xT", "xT/90"]

        # Normalise 0–100
        norm = merged.copy()
        for m in metrics:
            max_val = norm[m].max()
            norm[m] = (norm[m] / max_val * 100).round(1) if max_val > 0 else 0

        colors = ["#e74c3c", "#3498db", "#2ecc71"]
        fig = go.Figure()

        for i, player in enumerate(selected_players[:3]):
            row = norm[norm["player_name"] == player]
            if row.empty:
                continue
            vals = row[metrics].values[0].tolist()
            vals += [vals[0]]  # close polygon
            fig.add_trace(go.Scatterpolar(
                r=vals,
                theta=labels + [labels[0]],
                fill="toself",
                name=player,
                line_color=colors[i],
                fillcolor=colors[i],
                opacity=0.3
            ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100],
                                tickfont=dict(color="white")),
                angularaxis=dict(tickfont=dict(color="white"))
            ),
            template="plotly_dark",
            height=520,
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)