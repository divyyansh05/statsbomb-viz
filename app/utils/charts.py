"""
Chart rendering functions for the Streamlit dashboard.
mplsoccer-based static charts returned as matplotlib figures.
"""
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from mplsoccer import Pitch, VerticalPitch


def plot_xg_timeline(df: pd.DataFrame, home_team: str, away_team: str) -> plt.Figure:
    """Cumulative xG timeline for both teams over 90 minutes."""
    fig, ax = plt.subplots(figsize=(12, 5), facecolor="#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    for team, color, label in [
        (home_team, "#e74c3c", home_team),
        (away_team, "#3498db", away_team)
    ]:
        t = df[df["team_name"] == team].sort_values("minute")
        if t.empty:
            continue
        minutes = [0] + t["minute"].tolist() + [90]
        xg_vals = [0] + t["cumulative_xg"].tolist() + [t["cumulative_xg"].iloc[-1]]
        ax.step(minutes, xg_vals, where="post", color=color, linewidth=2.5, label=label)
        ax.fill_between(minutes, xg_vals, step="post", alpha=0.15, color=color)

    ax.set_xlabel("Minute", color="white", fontsize=11)
    ax.set_ylabel("Cumulative xG", color="white", fontsize=11)
    ax.set_title("xG Timeline", color="white", fontsize=14, fontweight="bold")
    ax.tick_params(colors="white")
    ax.spines[:].set_color("#444")
    ax.legend(facecolor="#2a2a3e", labelcolor="white", fontsize=10)
    ax.set_xlim(0, 95)
    ax.set_ylim(bottom=0)
    ax.axvline(45, color="#555", linestyle="--", linewidth=0.8)
    plt.tight_layout()
    return fig


def plot_shot_map(df: pd.DataFrame, home_team: str, away_team: str) -> plt.Figure:
    """Shot map with xG bubble sizing."""
    pitch = VerticalPitch(
        pitch_type="statsbomb",
        pitch_color="#1a1a2e",
        line_color="#555",
        half=True
    )
    fig, ax = pitch.draw(figsize=(10, 8))
    fig.patch.set_facecolor("#1a1a2e")

    colors = {home_team: "#e74c3c", away_team: "#3498db"}
    markers = {True: "o", False: "x"}

    for _, shot in df.iterrows():
        color = colors.get(shot["team_name"], "#888")
        marker = "o" if shot["is_goal"] else "x"
        size = max(50, shot["xg"] * 800) if pd.notna(shot["xg"]) else 50
        ax.scatter(
            shot["location_y"], shot["location_x"],
            c=color, s=size,
            marker=marker,
            alpha=0.8 if shot["is_goal"] else 0.5,
            edgecolors="white" if shot["is_goal"] else "none",
            linewidths=1.5,
            zorder=5
        )

    patches = [mpatches.Patch(color=c, label=t) for t, c in colors.items()]
    ax.legend(handles=patches, facecolor="#2a2a3e",
              labelcolor="white", loc="lower center", fontsize=9)
    ax.set_title("Shot Map (bubble = xG)", color="white",
                 fontsize=13, fontweight="bold", pad=10)
    plt.tight_layout()
    return fig


def plot_pass_network(nodes: pd.DataFrame, edges: pd.DataFrame, team: str) -> plt.Figure:
    """Pass network with nodes sized by pass volume."""
    pitch = Pitch(
        pitch_type="statsbomb",
        pitch_color="#1a1a2e",
        line_color="#555"
    )
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.patch.set_facecolor("#1a1a2e")

    if nodes.empty:
        ax.text(60, 40, "No data", color="white", ha="center", fontsize=12)
        return fig

    max_passes = edges["pass_count"].max() if not edges.empty else 1

    for _, edge in edges.iterrows():
        src = nodes[nodes["player_id"] == edge["passer_id"]]
        tgt = nodes[nodes["player_id"] == edge["recipient_id"]]
        if src.empty or tgt.empty:
            continue
        lw = (edge["pass_count"] / max_passes) * 6
        ax.plot(
            [src["avg_x"].values[0], tgt["avg_x"].values[0]],
            [src["avg_y"].values[0], tgt["avg_y"].values[0]],
            color="#aaaaaa", linewidth=lw, alpha=0.5, zorder=3
        )

    max_count = nodes["pass_count"].max() if not nodes.empty else 1
    for _, node in nodes.iterrows():
        size = (node["pass_count"] / max_count) * 1200 + 100
        ax.scatter(
            node["avg_x"], node["avg_y"],
            s=size, color="#e74c3c",
            edgecolors="white", linewidths=1.5, zorder=5
        )
        ax.text(
            node["avg_x"], node["avg_y"] - 4,
            node["player_name"].split()[-1],
            color="white", fontsize=7,
            ha="center", va="top", zorder=6
        )

    ax.set_title(f"{team} — Pass Network", color="white",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig


def plot_formation(starters: pd.DataFrame, subs: pd.DataFrame, team: str) -> plt.Figure:
    """Average position map — starters on pitch, subs listed below."""
    pitch = Pitch(
        pitch_type="statsbomb",
        pitch_color="#1a1a2e",
        line_color="#555"
    )
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.patch.set_facecolor("#1a1a2e")

    if starters.empty:
        ax.text(60, 40, "No data", color="white", ha="center", fontsize=12)
        return fig

    # Plot starters
    for _, row in starters.iterrows():
        if pd.isna(row.get("avg_x")) or pd.isna(row.get("avg_y")):
            continue
        ax.scatter(
            row["avg_x"], row["avg_y"],
            s=400, color="#e74c3c",
            edgecolors="white", linewidths=1.5, zorder=5
        )
        ax.text(
            row["avg_x"], row["avg_y"] - 4,
            row["player_name"].split()[-1],
            color="white", fontsize=7.5,
            ha="center", va="top", zorder=6
        )

    # Plot subs with different colour
    for _, row in subs.iterrows():
        if pd.isna(row.get("avg_x")) or pd.isna(row.get("avg_y")):
            continue
        ax.scatter(
            row["avg_x"], row["avg_y"],
            s=250, color="#f39c12",
            edgecolors="white", linewidths=1.2,
            marker="D", zorder=5
        )
        ax.text(
            row["avg_x"], row["avg_y"] - 4,
            row["player_name"].split()[-1],
            color="#f39c12", fontsize=7,
            ha="center", va="top", zorder=6
        )

    # Legend
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#e74c3c",
               markersize=10, label="Starter"),
        Line2D([0], [0], marker="D", color="w", markerfacecolor="#f39c12",
               markersize=8, label="Substitute"),
    ]
    ax.legend(handles=legend_elements, facecolor="#2a2a3e",
              labelcolor="white", loc="lower right", fontsize=9)

    ax.set_title(f"{team} — Average Positions", color="white",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig


def plot_pressure_heatmap(df: pd.DataFrame, team: str) -> plt.Figure:
    """Pressure event heatmap."""
    pitch = Pitch(
        pitch_type="statsbomb",
        pitch_color="#1a1a2e",
        line_color="#555"
    )
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.patch.set_facecolor("#1a1a2e")

    team_df = df[df["team"] == team] if "team" in df.columns else df

    if team_df.empty:
        ax.text(60, 40, "No pressure data", color="white", ha="center", fontsize=12)
        return fig

    pitch.kdeplot(
        team_df["location_x"], team_df["location_y"],
        ax=ax, fill=True,
        cmap="Reds", levels=50, alpha=0.7
    )

    ax.set_title(f"{team} — Pressure Heatmap", color="white",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    return fig