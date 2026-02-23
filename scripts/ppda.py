"""
PPDA — Passes Per Defensive Action.

Measures pressing intensity per team per match.
Lower = more aggressive press.

Formula:
    PPDA = opponent_passes_in_defensive_zone / defensive_actions_in_defensive_zone

Defensive zone = x > 48 (opponent's 60% of pitch on 120-length pitch).
Defensive actions = tackles, interceptions, fouls won.
"""

import sys
import pandas as pd
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.utils import get_db_connection

PRESS_ZONE_X = 48.0  # Only count events in opponent's 60% of pitch


def main():
    print("=" * 60)
    print("  PPDA — Passes Per Defensive Action")
    print("=" * 60)

    con = get_db_connection(read_only=False)

    print("\nComputing PPDA per team per match...")

    # Opponent passes allowed in defensive zone
    opp_passes = con.execute("""
        SELECT
            e.match_id,
            e.team_id,
            e.team,
            COUNT(*) as opp_passes
        FROM fact_events e
        JOIN dim_match m ON e.match_id = m.match_id
        WHERE e.type = 'Pass'
          AND e.location_x > ?
          -- Count passes by the OPPONENT (not this team)
          AND e.team != CASE 
                WHEN m.home_team = e.team THEN m.away_team 
                ELSE m.home_team 
              END
        GROUP BY e.match_id, e.team_id, e.team
    """, [PRESS_ZONE_X]).df()

    # Simpler correct approach — count per team per match directly
    # Passes allowed = passes made by opponent in YOUR defensive zone
    # = passes made by team X in zone where team Y is defending

    # Step 1: passes made by each team in the opponent's defensive zone
    passes_in_zone = con.execute("""
        SELECT
            match_id,
            team_id,
            team,
            COUNT(*) as passes_made
        FROM fact_events
        WHERE type = 'Pass'
          AND location_x > ?
        GROUP BY match_id, team_id, team
    """, [PRESS_ZONE_X]).df()

    # Step 2: defensive actions made by each team in their defensive zone
    # (tackles, interceptions, fouls — where THEY are pressing)
    def_actions = con.execute("""
        SELECT
            match_id,
            team_id,
            team,
            COUNT(*) as def_actions
        FROM fact_events
        WHERE type IN ('Tackle', 'Interception', 'Foul Committed', 'Ball Recovery')
          AND location_x > ?
        GROUP BY match_id, team_id, team
    """, [PRESS_ZONE_X]).df()

    # Step 3: join — for each team, opponent passes = passes made by OTHER team
    # Get both teams per match
    match_teams = con.execute("""
        SELECT DISTINCT match_id, team_id, team
        FROM fact_events
        WHERE team IS NOT NULL
    """).df()

    # Self-join to get opponent passes
    match_teams2 = match_teams.copy()
    match_teams2.columns = ["match_id", "opp_team_id", "opp_team"]

    pairs = match_teams.merge(match_teams2, on="match_id")
    pairs = pairs[pairs["team_id"] != pairs["opp_team_id"]]

    # Attach passes made by opponent
    pairs = pairs.merge(
        passes_in_zone.rename(columns={"team_id":"opp_team_id","passes_made":"opp_passes_in_zone"})[
            ["match_id","opp_team_id","opp_passes_in_zone"]],
        on=["match_id","opp_team_id"],
        how="left"
    )

    # Attach own defensive actions
    pairs = pairs.merge(
        def_actions[["match_id","team_id","def_actions"]],
        on=["match_id","team_id"],
        how="left"
    )

    pairs["opp_passes_in_zone"] = pairs["opp_passes_in_zone"].fillna(0)
    pairs["def_actions"]        = pairs["def_actions"].fillna(1)  # avoid div/0

    # PPDA = opponent passes / own defensive actions
    pairs["ppda"] = (pairs["opp_passes_in_zone"] / pairs["def_actions"]).round(2)

    ppda_match = pairs[["match_id","team_id","team","opp_passes_in_zone","def_actions","ppda"]].copy()

    # Season average PPDA per team
    ppda_team = ppda_match.groupby(["team_id","team"]).agg(
        avg_ppda        = ("ppda", "mean"),
        matches         = ("match_id", "nunique"),
        total_def_actions = ("def_actions", "sum"),
    ).reset_index()
    ppda_team["avg_ppda"] = ppda_team["avg_ppda"].round(2)
    ppda_team = ppda_team.sort_values("avg_ppda")

    print("\nTop 10 pressing teams (lowest PPDA = most aggressive press):")
    print(ppda_team.head(10)[["team","matches","avg_ppda","total_def_actions"]].to_string(index=False))

    print("\nBottom 10 pressing teams (highest PPDA = least pressing):")
    print(ppda_team.tail(10)[["team","matches","avg_ppda","total_def_actions"]].to_string(index=False))

    # Save to DuckDB
    print("\nSaving to DuckDB...")
    con.execute("DROP TABLE IF EXISTS gold_ppda_match")
    con.execute("CREATE TABLE gold_ppda_match AS SELECT * FROM ppda_match")
    print(f"  gold_ppda_match: {len(ppda_match):,} rows")

    con.execute("DROP TABLE IF EXISTS gold_ppda_team")
    con.execute("CREATE TABLE gold_ppda_team AS SELECT * FROM ppda_team")
    print(f"  gold_ppda_team: {len(ppda_team)} teams")

    # Also add ppda to gold_team_stats
    print("  Merging PPDA into gold_team_stats...")
    ts = con.execute("SELECT * FROM gold_team_stats").df()
    ts = ts.merge(
        ppda_match[["match_id","team_id","ppda"]],
        on=["match_id","team_id"],
        how="left"
    )
    con.execute("DROP TABLE IF EXISTS gold_team_stats")
    con.execute("CREATE TABLE gold_team_stats AS SELECT * FROM ts")
    print(f"  gold_team_stats updated with ppda column")

    con.close()
    print("\n✅ Done.")


if __name__ == "__main__":
    main()