"""
Match Report — xG timeline, shot map, pass network, formation, pressure heatmap.
"""
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db import (
    get_competitions, get_matches, get_match_teams,
    get_xg_timeline, get_shot_map, get_pass_network,
    get_formation_starters, get_formation_subs,
    get_pressure_events, get_team_id
)
from utils.charts import (
    plot_xg_timeline, plot_shot_map,
    plot_pass_network, plot_formation, plot_pressure_heatmap
)

st.set_page_config(page_title="Match Report", page_icon="📋", layout="wide")
st.title("📋 Match Report")

# ── Sidebar selectors ──────────────────────────────────────────────────────────
st.sidebar.header("Select Match")

comps = get_competitions()
comp_labels = (comps["competition_name"] + " — " + comps["season_name"]).tolist()
comp_idx = st.sidebar.selectbox("Competition", range(len(comp_labels)),
                                 format_func=lambda i: comp_labels[i])
selected_comp = comps.iloc[comp_idx]

matches = get_matches(int(selected_comp["competition_id"]),
                      int(selected_comp["season_id"]))
match_labels = (
    matches["match_date"].astype(str) + " | " +
    matches["home_team"] + " " +
    matches["home_score"].astype(str) + " - " +
    matches["away_score"].astype(str) + " " +
    matches["away_team"]
).tolist()
match_idx = st.sidebar.selectbox("Match", range(len(match_labels)),
                                  format_func=lambda i: match_labels[i])
selected_match = matches.iloc[match_idx]
match_id      = int(selected_match["match_id"])
home_team     = selected_match["home_team"]
away_team     = selected_match["away_team"]

# Resolve team IDs once — used for pass network + formation queries
home_id = get_team_id(match_id, home_team)
away_id = get_team_id(match_id, away_team)

# ── Match header ───────────────────────────────────────────────────────────────
st.markdown(f"""
<div style='background:#1a1a2e;padding:20px;border-radius:10px;
            text-align:center;margin-bottom:20px'>
    <h2 style='color:white;margin:0'>
        {home_team}
        <span style='color:#e74c3c'> {int(selected_match['home_score'])} </span>
        <span style='color:#aaa'>—</span>
        <span style='color:#3498db'> {int(selected_match['away_score'])} </span>
        {away_team}
    </h2>
    <p style='color:#aaa;margin:5px 0 0 0'>
        {selected_match['match_date']} &nbsp;|&nbsp;
        {selected_comp['competition_name']} {selected_comp['season_name']}
    </p>
</div>
""", unsafe_allow_html=True)

# ── xG summary metrics ─────────────────────────────────────────────────────────
shot_df = get_shot_map(match_id)

if not shot_df.empty:
    home_shots = shot_df[shot_df["team_name"] == home_team]
    away_shots = shot_df[shot_df["team_name"] == away_team]

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric(f"{home_team} xG",
                f"{home_shots['xg'].sum():.2f}" if not home_shots.empty else "—")
    col2.metric(f"{home_team} Shots", len(home_shots))
    col3.metric(f"{home_team} Goals",
                int(home_shots["is_goal"].sum()) if not home_shots.empty else 0)
    col4.metric(f"{away_team} xG",
                f"{away_shots['xg'].sum():.2f}" if not away_shots.empty else "—")
    col5.metric(f"{away_team} Shots", len(away_shots))
    col6.metric(f"{away_team} Goals",
                int(away_shots["is_goal"].sum()) if not away_shots.empty else 0)

st.markdown("---")

# ── xG Timeline ───────────────────────────────────────────────────────────────
st.subheader("xG Timeline")
xg_df = get_xg_timeline(match_id)
if not xg_df.empty:
    fig = plot_xg_timeline(xg_df, home_team, away_team)
    st.pyplot(fig, use_container_width=True)
else:
    st.info("No xG timeline data for this match.")

st.markdown("---")

# ── Shot Map ──────────────────────────────────────────────────────────────────
st.subheader("Shot Map")
if not shot_df.empty:
    fig = plot_shot_map(shot_df, home_team, away_team)
    st.pyplot(fig, use_container_width=True)
else:
    st.info("No shot data for this match.")

st.markdown("---")

# ── Pass Networks ─────────────────────────────────────────────────────────────
st.subheader("Pass Networks")
col_home, col_away = st.columns(2)

with col_home:
    if home_id:
        nodes, edges = get_pass_network(match_id, home_id)
        fig = plot_pass_network(nodes, edges, home_team)
        st.pyplot(fig, use_container_width=True)
    else:
        st.info(f"No pass network data for {home_team}.")

with col_away:
    if away_id:
        nodes, edges = get_pass_network(match_id, away_id)
        fig = plot_pass_network(nodes, edges, away_team)
        st.pyplot(fig, use_container_width=True)
    else:
        st.info(f"No pass network data for {away_team}.")

st.markdown("---")




# ── Formations ────────────────────────────────────────────────────────────────
st.subheader("Average Positions")
col_home, col_away = st.columns(2)

with col_home:
    if home_id:
        starters = get_formation_starters(match_id, home_id)
        subs     = get_formation_subs(match_id, home_id)
        fig = plot_formation(starters, subs, home_team)
        st.pyplot(fig, use_container_width=True)
        if not subs.empty:
            st.caption(f"🟠 Substitutes who played: "
                       f"{', '.join(subs['player_name'].tolist())}")
    else:
        st.info(f"No formation data for {home_team}.")

with col_away:
    if away_id:
        starters = get_formation_starters(match_id, away_id)
        subs     = get_formation_subs(match_id, away_id)
        fig = plot_formation(starters, subs, away_team)
        st.pyplot(fig, use_container_width=True)
        if not subs.empty:
            st.caption(f"🟠 Substitutes who played: "
                       f"{', '.join(subs['player_name'].tolist())}")
    else:
        st.info(f"No formation data for {away_team}.")


# ── Pressure Heatmap ──────────────────────────────────────────────────────────
st.subheader("Pressure Heatmap")
pressure_df = get_pressure_events(match_id)

if not pressure_df.empty:
    team_choice = st.radio("Select team", [home_team, away_team], horizontal=True)
    pressure_filtered = pressure_df[pressure_df["team"] == team_choice]
    fig = plot_pressure_heatmap(pressure_filtered, team_choice)
    st.pyplot(fig, use_container_width=True)
else:
    st.info("No pressure data for this match.")