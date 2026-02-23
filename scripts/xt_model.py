"""
xT Model — Expected Threat via Markov chain value iteration.

Theory:
    Divide the pitch into a 16x12 grid (192 zones).
    Each zone has a value xT(z) = probability that possession in zone z
    leads to a goal within the next few actions.

    Solved via value iteration:
        xT(z) = P(shot|z) * P(goal|shot,z)
              + P(move|z) * sum_z'[ P(move to z'|z) * xT(z') ]

    xT-added per action:
        xT_added = xT(end_zone) - xT(start_zone)

    Only completed passes and carries are used — incomplete passes
    don't represent a successful threat progression.
"""

import sys
import numpy as np
import pandas as pd
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils import get_db_connection

GRID_X   = 16    # zones along pitch length
GRID_Y   = 12    # zones along pitch width
PITCH_X  = 120.0
PITCH_Y  = 80.0
ITERS    = 10    # value iteration convergence (usually stable after 5-7)


# ── Grid helpers ───────────────────────────────────────────────────────────────

def to_grid(x: pd.Series, y: pd.Series):
    """Convert pitch coordinates to grid indices (col, row)."""
    col = (x / PITCH_X * GRID_X).clip(0, GRID_X - 1).astype(int)
    row = (y / PITCH_Y * GRID_Y).clip(0, GRID_Y - 1).astype(int)
    return col, row


def zone_index(col: np.ndarray, row: np.ndarray) -> np.ndarray:
    return row * GRID_X + col


# ── Load data ──────────────────────────────────────────────────────────────────

def load_data():
    print("Loading passes and carries from DuckDB...")
    con = get_db_connection(read_only=True)

    passes = con.execute("""
        SELECT p.event_id, p.match_id, p.player_id, p.team_id,
               p.location_x, p.location_y,
               p.end_location_x, p.end_location_y,
               p.is_completed,
               p.minute, p.second, p.period,
               pl.player_name
        FROM fact_passes p
        LEFT JOIN dim_player pl ON p.player_id = pl.player_id
        WHERE p.location_x IS NOT NULL
          AND p.end_location_x IS NOT NULL
          AND p.pass_type NOT IN ('Kick Off','Goal Kick','Corner','Throw-in')
            OR p.pass_type IS NULL
    """).df()

    carries = con.execute("""
        SELECT c.event_id, c.match_id, c.player_id, c.team_id,
               c.location_x, c.location_y,
               c.end_location_x, c.end_location_y,
               c.minute, c.second, c.period,
               pl.player_name
        FROM fact_carries c
        LEFT JOIN dim_player pl ON c.player_id = pl.player_id
        WHERE c.location_x IS NOT NULL
          AND c.end_location_x IS NOT NULL
    """).df()

    shots = con.execute("""
        SELECT location_x, location_y, is_goal
        FROM fact_shots
        WHERE location_x IS NOT NULL
    """).df()

    # Match minutes played per player (for per-90 normalisation)
    minutes = con.execute("""
        SELECT player_id, COUNT(DISTINCT match_id) as matches_played
        FROM fact_events
        WHERE player_id IS NOT NULL
        GROUP BY player_id
    """).df()

    con.close()

    print(f"  Passes:  {len(passes):,}  ({passes['is_completed'].sum():,} completed)")
    print(f"  Carries: {len(carries):,}")
    print(f"  Shots:   {len(shots):,}")

    return passes, carries, shots, minutes


# ── Build xT grid ──────────────────────────────────────────────────────────────

def build_xt_grid(passes, carries, shots):
    print("\nBuilding xT grid via value iteration...")

    # --- Shot probability grid: P(shot | zone) ---
    # Use all events implicitly via shots vs passes+carries counts
    shot_col, shot_row = to_grid(shots["location_x"], shots["location_y"])
    shot_counts = np.zeros((GRID_Y, GRID_X))
    np.add.at(shot_counts, (shot_row.values, shot_col.values), 1)

    completed_passes = passes[passes["is_completed"]].copy()
    pass_col, pass_row = to_grid(completed_passes["location_x"],
                                  completed_passes["location_y"])
    carry_col, carry_row = to_grid(carries["location_x"], carries["location_y"])

    move_counts = np.zeros((GRID_Y, GRID_X))
    np.add.at(move_counts, (pass_row.values, pass_col.values), 1)
    np.add.at(move_counts, (carry_row.values, carry_col.values), 1)

    total_counts = shot_counts + move_counts + 1e-6   # avoid div/0
    p_shot  = shot_counts  / total_counts
    p_move  = move_counts  / total_counts

    # --- Goal probability grid: P(goal | shot from zone) ---
    goal_shots = shots[shots["is_goal"] == True]
    goal_col, goal_row = to_grid(goal_shots["location_x"], goal_shots["location_y"])
    goal_counts = np.zeros((GRID_Y, GRID_X))
    np.add.at(goal_counts, (goal_row.values, goal_col.values), 1)
    p_goal_given_shot = goal_counts / (shot_counts + 1e-6)

    # --- Transition matrix: P(move to z' | move from z) ---
    print("  Computing transition matrix...")
    T = np.zeros((GRID_Y * GRID_X, GRID_Y * GRID_X))

    all_moves = pd.concat([
        completed_passes[["location_x","location_y","end_location_x","end_location_y"]],
        carries[["location_x","location_y","end_location_x","end_location_y"]]
    ], ignore_index=True)

    start_col, start_row = to_grid(all_moves["location_x"], all_moves["location_y"])
    end_col,   end_row   = to_grid(all_moves["end_location_x"], all_moves["end_location_y"])

    start_idx = zone_index(start_col.values, start_row.values)
    end_idx   = zone_index(end_col.values,   end_row.values)

    np.add.at(T, (start_idx, end_idx), 1)

    # Normalise rows → probabilities
    row_sums = T.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    T = T / row_sums

    # --- Value iteration ---
    print(f"  Running value iteration ({ITERS} iterations)...")
    xT = np.zeros((GRID_Y, GRID_X))

    for i in range(ITERS):
        xT_flat = xT.flatten()
        xT_new = (p_shot * p_goal_given_shot) + (p_move * (T @ xT_flat).reshape(GRID_Y, GRID_X))
        delta = np.abs(xT_new - xT).max()
        xT = xT_new
        print(f"    iter {i+1}: max delta = {delta:.6f}")

    print(f"\n  xT grid complete.")
    print(f"  Max xT zone: {xT.max():.4f}  (near-post close range)")
    print(f"  Min xT zone: {xT.min():.4f}  (own half deep)")

    return xT, T


# ── Assign xT-added per action ─────────────────────────────────────────────────

def assign_xt(df: pd.DataFrame, xT: np.ndarray, action_type: str) -> pd.DataFrame:
    """Assign xT_start, xT_end, xT_added to each action."""
    start_col, start_row = to_grid(df["location_x"], df["location_y"])
    end_col,   end_row   = to_grid(df["end_location_x"], df["end_location_y"])

    df = df.copy()
    df["xt_start"] = xT[start_row.values, start_col.values]
    df["xt_end"]   = xT[end_row.values,   end_col.values]
    df["xt_added"] = df["xt_end"] - df["xt_start"]
    df["action"]   = action_type
    return df


# ── Build gold_xt_player table ─────────────────────────────────────────────────

def build_gold_xt_player(passes, carries, xT, minutes):
    print("\nAssigning xT-added to passes and carries...")

    completed_passes = passes[passes["is_completed"]].copy()
    passes_xt  = assign_xt(completed_passes, xT, "pass")
    carries_xt = assign_xt(carries, xT, "carry")

    all_actions = pd.concat([
        passes_xt[["player_id","player_name","match_id","action","xt_added"]],
        carries_xt[["player_id","player_name","match_id","action","xt_added"]]
    ], ignore_index=True)

    # Aggregate per player
    player_xt = all_actions.groupby(["player_id","player_name"]).agg(
        total_xt_added    = ("xt_added", "sum"),
        xt_passes         = ("xt_added", lambda x: x[all_actions.loc[x.index,"action"]=="pass"].sum()),
        xt_carries        = ("xt_added", lambda x: x[all_actions.loc[x.index,"action"]=="carry"].sum()),
        actions_count     = ("xt_added", "count"),
        matches_played    = ("match_id", "nunique"),
    ).reset_index()

    # Per-90 normalisation (assume ~90 mins per match as approximation)
    player_xt["xt_per_90"] = (
        player_xt["total_xt_added"] / player_xt["matches_played"]
    ).round(4)

    player_xt["total_xt_added"] = player_xt["total_xt_added"].round(4)
    player_xt = player_xt[player_xt["matches_played"] >= 3]  # min 3 matches
    player_xt = player_xt.sort_values("total_xt_added", ascending=False)

    return player_xt


# ── Save results ───────────────────────────────────────────────────────────────

def save_results(xT: np.ndarray, player_xt: pd.DataFrame):
    print("\nSaving to DuckDB...")
    con = get_db_connection(read_only=False)

    # Save xT grid as flat table
    rows, cols = np.meshgrid(range(GRID_Y), range(GRID_X), indexing="ij")
    grid_df = pd.DataFrame({
        "grid_row":  rows.flatten(),
        "grid_col":  cols.flatten(),
        "zone_x_start": (cols.flatten() / GRID_X * PITCH_X),
        "zone_y_start": (rows.flatten() / GRID_Y * PITCH_Y),
        "xt_value":  xT.flatten().round(5)
    })
    con.execute("DROP TABLE IF EXISTS gold_xt_grid")
    con.execute("CREATE TABLE gold_xt_grid AS SELECT * FROM grid_df")
    print(f"  gold_xt_grid: {len(grid_df)} zones")

    # Save player xT table
    con.execute("DROP TABLE IF EXISTS gold_xt_player")
    con.execute("CREATE TABLE gold_xt_player AS SELECT * FROM player_xt")
    print(f"  gold_xt_player: {len(player_xt):,} players")

    con.close()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  StatsBomb xT Model — Expected Threat (Markov chain)")
    print("=" * 60)

    passes, carries, shots, minutes = load_data()
    xT, T = build_xt_grid(passes, carries, shots)

    player_xt = build_gold_xt_player(passes, carries, xT, minutes)

    print("\nTop 15 players by total xT-added:")
    print(player_xt[["player_name","matches_played","total_xt_added",
                      "xt_per_90","actions_count"]].head(15).to_string(index=False))

    save_results(xT, player_xt)
    print("\n✅ Done.")


if __name__ == "__main__":
    main()