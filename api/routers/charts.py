"""
Chart endpoints — return base64-encoded PNG images generated with mplsoccer.

Colour scheme:
  Background:  #0f1117
  Pitch:       #1a1d2e
  Pitch lines: #4a4e69
  Home team:   #52b788
  Away team:   #e63946
  Text:        #e0e0e0
"""
import base64
import io
from typing import Optional

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from mplsoccer import Pitch, VerticalPitch

from api.dependencies import get_db

router = APIRouter()

# ── Colour constants ───────────────────────────────────────────────────────────
BG       = "#0f1117"
PITCH_C  = "#1a1d2e"
LINE_C   = "#4a4e69"
HOME_C   = "#52b788"
AWAY_C   = "#e63946"
TEXT_C   = "#e0e0e0"


def _fig_to_b64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return encoded


def _get_team_colour(match_id: int, team_id: int, db) -> str:
    """Return HOME_C or AWAY_C depending on whether team is home or away."""
    row = db.execute("""
        SELECT home_team, away_team FROM dim_match WHERE match_id = ?
    """, [match_id]).fetchone()
    if not row:
        return HOME_C
    home_team, away_team = row
    # Get team name
    team_name = db.execute("""
        SELECT DISTINCT team FROM fact_events
        WHERE match_id = ? AND team_id = ? LIMIT 1
    """, [match_id, team_id]).fetchone()
    if not team_name:
        return HOME_C
    return HOME_C if team_name[0] == home_team else AWAY_C


# ── 1. Formation / Average Position Chart ─────────────────────────────────────

@router.get("/charts/{match_id}/formation/{team_id}")
def formation_chart(
    match_id: int,
    team_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    rows = db.execute("""
        SELECT player_name, jersey_number, avg_x, avg_y, touch_count
        FROM gold_formation_positions
        WHERE match_id = ? AND team_id = ?
        ORDER BY avg_x
    """, [match_id, team_id]).fetchall()

    if not rows:
        raise HTTPException(404, f"No formation data for match {match_id} team {team_id}")

    df = pd.DataFrame(rows, columns=["player_name", "jersey_number", "avg_x", "avg_y", "touch_count"])
    colour = _get_team_colour(match_id, team_id, db)

    team_name = db.execute("""
        SELECT DISTINCT team FROM fact_events WHERE match_id=? AND team_id=? LIMIT 1
    """, [match_id, team_id]).fetchone()
    title = f"{team_name[0]} — Average Positions" if team_name else "Average Positions"

    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_C, line_color=LINE_C)
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PITCH_C)

    for _, r in df.iterrows():
        x, y = r["avg_x"], r["avg_y"]
        if pd.isna(x) or pd.isna(y):
            continue
        size = max(200, min(r["touch_count"] * 3, 1200))
        ax.scatter(x, y, s=size, color=colour, edgecolors=TEXT_C, linewidths=1.5, zorder=4)
        label = str(int(r["jersey_number"])) if pd.notna(r["jersey_number"]) else ""
        ax.text(x, y, label, ha="center", va="center",
                color="white", fontsize=8, fontweight="bold", zorder=5)
        ax.text(x, y - 3.5, r["player_name"].split()[-1], ha="center",
                color=TEXT_C, fontsize=6.5, zorder=5)

    ax.set_title(title, color=TEXT_C, fontsize=13, pad=10)
    fig.tight_layout()

    return {"image": _fig_to_b64(fig), "match_id": match_id, "team_id": team_id}


# ── 2. Pass Network Chart ──────────────────────────────────────────────────────

@router.get("/charts/{match_id}/pass_network/{team_id}")
def pass_network_chart(
    match_id: int,
    team_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    nodes = db.execute("""
        SELECT player_id, player_name, avg_x, avg_y, pass_count
        FROM gold_pass_network_nodes
        WHERE match_id = ? AND team_id = ?
    """, [match_id, team_id]).fetchall()

    edges = db.execute("""
        SELECT passer_id, recipient_id, pass_count,
               avg_start_x, avg_start_y, avg_end_x, avg_end_y
        FROM gold_pass_network_edges
        WHERE match_id = ? AND team_id = ?
    """, [match_id, team_id]).fetchall()

    if not nodes:
        raise HTTPException(404, f"No pass network data for match {match_id} team {team_id}")

    df_nodes = pd.DataFrame(nodes, columns=["player_id", "player_name", "avg_x", "avg_y", "pass_count"])
    df_edges = pd.DataFrame(edges, columns=["passer_id", "recipient_id", "pass_count",
                                             "avg_start_x", "avg_start_y", "avg_end_x", "avg_end_y"])

    colour = _get_team_colour(match_id, team_id, db)
    team_name = db.execute("""
        SELECT DISTINCT team FROM fact_events WHERE match_id=? AND team_id=? LIMIT 1
    """, [match_id, team_id]).fetchone()
    title = f"{team_name[0]} — Pass Network" if team_name else "Pass Network"

    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_C, line_color=LINE_C)
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PITCH_C)

    max_passes = df_edges["pass_count"].max() if not df_edges.empty else 1

    # Draw edges
    for _, e in df_edges.iterrows():
        alpha = 0.2 + 0.6 * (e["pass_count"] / max_passes)
        lw = 0.5 + 3.5 * (e["pass_count"] / max_passes)
        ax.annotate("",
            xy=(e["avg_end_x"], e["avg_end_y"]),
            xytext=(e["avg_start_x"], e["avg_start_y"]),
            arrowprops=dict(arrowstyle="-|>", color=colour,
                            lw=lw, alpha=alpha, mutation_scale=12),
            zorder=2)

    # Draw nodes
    node_pos = {r["player_id"]: (r["avg_x"], r["avg_y"]) for _, r in df_nodes.iterrows()}
    max_count = df_nodes["pass_count"].max() if not df_nodes.empty else 1
    for _, n in df_nodes.iterrows():
        x, y = n["avg_x"], n["avg_y"]
        if pd.isna(x) or pd.isna(y):
            continue
        size = 200 + 800 * (n["pass_count"] / max_count)
        ax.scatter(x, y, s=size, color=colour, edgecolors=TEXT_C, linewidths=1.5, zorder=4)
        last = n["player_name"].split()[-1] if isinstance(n["player_name"], str) else ""
        ax.text(x, y - 3.5, last, ha="center", color=TEXT_C, fontsize=6.5, zorder=5)

    ax.set_title(title, color=TEXT_C, fontsize=13, pad=10)
    fig.tight_layout()

    return {"image": _fig_to_b64(fig), "match_id": match_id, "team_id": team_id}


# ── 3. Shot Map ────────────────────────────────────────────────────────────────

@router.get("/charts/{match_id}/shot_map")
def shot_map_chart(
    match_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    rows = db.execute("""
        SELECT team_id, team_name, player_name, location_x, location_y,
               xg, outcome, is_goal, body_part
        FROM gold_shot_map
        WHERE match_id = ?
        ORDER BY team_name
    """, [match_id]).fetchall()

    if not rows:
        raise HTTPException(404, f"No shot data for match {match_id}")

    df = pd.DataFrame(rows, columns=["team_id", "team_name", "player_name",
                                      "location_x", "location_y", "xg", "outcome",
                                      "is_goal", "body_part"])

    meta = db.execute("""
        SELECT home_team, away_team, home_score, away_score
        FROM dim_match WHERE match_id = ?
    """, [match_id]).fetchone()
    home_team, away_team = (meta[0], meta[1]) if meta else ("Home", "Away")
    score = f"{meta[2]}–{meta[3]}" if meta else ""

    pitch = VerticalPitch(pitch_type="statsbomb", pitch_color=PITCH_C,
                          line_color=LINE_C, half=True)
    fig, ax = pitch.draw(figsize=(9, 8))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PITCH_C)

    teams = df["team_name"].unique()
    colours = {home_team: HOME_C, away_team: AWAY_C}
    for team in teams:
        if team not in colours:
            colours[team] = HOME_C  # fallback

    for team, group in df.groupby("team_name"):
        c = colours.get(team, HOME_C)
        for _, r in group.iterrows():
            xg_val = r["xg"] if pd.notna(r["xg"]) else 0.01
            size = max(50, xg_val * 1000)
            marker = "*" if r["is_goal"] else "o"
            edge = "gold" if r["is_goal"] else LINE_C
            ax.scatter(r["location_x"], r["location_y"],
                       s=size, color=c, marker=marker,
                       edgecolors=edge, linewidths=1.5,
                       alpha=0.85, zorder=4)

    title = f"Shot Map  {home_team} {score} {away_team}"
    ax.set_title(title, color=TEXT_C, fontsize=12, pad=10)

    patches = [
        mpatches.Patch(color=HOME_C, label=home_team),
        mpatches.Patch(color=AWAY_C, label=away_team),
        mpatches.Patch(color="gold", label="Goal (★)"),
    ]
    ax.legend(handles=patches, loc="lower right",
              facecolor=BG, edgecolor=LINE_C, labelcolor=TEXT_C, fontsize=8)
    fig.tight_layout()

    return {"image": _fig_to_b64(fig), "match_id": match_id}


# ── 4. xG Timeline ────────────────────────────────────────────────────────────

@router.get("/charts/{match_id}/xg_timeline")
def xg_timeline_chart(
    match_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    rows = db.execute("""
        SELECT team_id, team_name, period, minute, second,
               xg, is_goal, cumulative_xg
        FROM gold_xg_timeline
        WHERE match_id = ?
        ORDER BY team_id, period, minute, second
    """, [match_id]).fetchall()

    if not rows:
        raise HTTPException(404, f"No xG data for match {match_id}")

    df = pd.DataFrame(rows, columns=["team_id", "team_name", "period", "minute",
                                      "second", "xg", "is_goal", "cumulative_xg"])
    df["time"] = df["minute"] + df["second"] / 60 + (df["period"] - 1) * 45

    meta = db.execute("""
        SELECT home_team, away_team, home_score, away_score
        FROM dim_match WHERE match_id = ?
    """, [match_id]).fetchone()
    home_team, away_team = (meta[0], meta[1]) if meta else ("Home", "Away")
    score = f"{meta[2]}–{meta[3]}" if meta else ""

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    teams = df["team_name"].unique()
    colours = {home_team: HOME_C, away_team: AWAY_C}

    for team, group in df.groupby("team_name"):
        c = colours.get(team, HOME_C)
        # Start from 0
        t_vals = [0] + list(group["time"])
        xg_vals = [0] + list(group["cumulative_xg"])
        ax.step(t_vals, xg_vals, where="post", color=c, linewidth=2.5, label=team)
        # Mark goals
        goals = group[group["is_goal"] == True]
        for _, g in goals.iterrows():
            ax.axvline(g["time"], color=c, alpha=0.3, linewidth=1, linestyle="--")
            ax.scatter(g["time"], g["cumulative_xg"], color=c,
                       s=120, marker="*", edgecolors="white", linewidths=0.8, zorder=5)

    # Period separator
    ax.axvline(45, color=LINE_C, linestyle=":", linewidth=1, alpha=0.7)
    ax.text(45.5, ax.get_ylim()[1] * 0.95, "HT", color=TEXT_C, fontsize=8, alpha=0.7)

    ax.set_xlabel("Minute", color=TEXT_C, fontsize=10)
    ax.set_ylabel("Cumulative xG", color=TEXT_C, fontsize=10)
    ax.tick_params(colors=TEXT_C)
    for spine in ax.spines.values():
        spine.set_edgecolor(LINE_C)
    ax.set_title(f"xG Timeline — {home_team} {score} {away_team}", color=TEXT_C, fontsize=13)
    ax.legend(facecolor=BG, edgecolor=LINE_C, labelcolor=TEXT_C, fontsize=9)
    ax.grid(True, color=LINE_C, alpha=0.3, linestyle="--")
    fig.tight_layout()

    return {"image": _fig_to_b64(fig), "match_id": match_id}


# ── 5. Pressure Heatmap ───────────────────────────────────────────────────────

@router.get("/charts/{match_id}/pressure_heatmap/{team_id}")
def pressure_heatmap_chart(
    match_id: int,
    team_id: int,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    rows = db.execute("""
        SELECT location_x, location_y
        FROM fact_events
        WHERE match_id = ? AND team_id = ? AND type = 'Pressure'
          AND location_x IS NOT NULL AND location_y IS NOT NULL
    """, [match_id, team_id]).fetchall()

    if not rows:
        raise HTTPException(404, f"No pressure data for match {match_id} team {team_id}")

    df = pd.DataFrame(rows, columns=["x", "y"])

    team_name = db.execute("""
        SELECT DISTINCT team FROM fact_events WHERE match_id=? AND team_id=? LIMIT 1
    """, [match_id, team_id]).fetchone()
    title = f"{team_name[0]} — Pressure Heatmap" if team_name else "Pressure Heatmap"

    colour = _get_team_colour(match_id, team_id, db)

    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_C, line_color=LINE_C)
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PITCH_C)

    # KDE heatmap
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list(
        "pressure", [PITCH_C, colour], N=256
    )
    pitch.kdeplot(df["x"], df["y"], ax=ax,
                  cmap=cmap, fill=True, alpha=0.65,
                  bw_adjust=0.6, zorder=2)

    ax.set_title(title, color=TEXT_C, fontsize=13, pad=10)
    ax.text(60, 1, f"n={len(df)} pressures", color=TEXT_C, fontsize=8, alpha=0.7)
    fig.tight_layout()

    return {"image": _fig_to_b64(fig), "match_id": match_id, "team_id": team_id}
