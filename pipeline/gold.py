"""
Gold layer: builds aggregation tables from silver data in DuckDB.
Drops and recreates each table (idempotent).
"""
from pipeline.logger import get_logger
from pipeline.utils import get_db_connection

log = get_logger(__name__)


def _exec(con, sql: str) -> None:
    con.execute(sql)


def build_gold_xg_timeline(con) -> None:
    _exec(con, "DROP TABLE IF EXISTS gold_xg_timeline")
    _exec(con, """
        CREATE TABLE gold_xg_timeline AS
        SELECT
            s.match_id,
            s.team_id,
            e.team AS team_name,
            s.period,
            s.minute,
            s.second,
            s.xg,
            s.is_goal,
            s.outcome,
            SUM(s.xg) OVER (
                PARTITION BY s.match_id, s.team_id
                ORDER BY s.period, s.minute, s.second
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_xg
        FROM fact_shots s
        LEFT JOIN (
            SELECT DISTINCT match_id, team_id, team
            FROM fact_events
        ) e ON s.match_id = e.match_id AND s.team_id = e.team_id
        ORDER BY s.match_id, s.team_id, s.period, s.minute, s.second
    """)
    count = con.execute("SELECT COUNT(*) FROM gold_xg_timeline").fetchone()[0]
    log.info(f"  gold_xg_timeline: {count} rows")


def build_gold_pass_network_nodes(con) -> None:
    _exec(con, "DROP TABLE IF EXISTS gold_pass_network_nodes")
    _exec(con, """
        CREATE TABLE gold_pass_network_nodes AS
        SELECT
            p.match_id,
            p.team_id,
            p.player_id,
            e.player AS player_name,
            AVG(p.location_x) AS avg_x,
            AVG(p.location_y) AS avg_y,
            COUNT(*) AS pass_count
        FROM fact_passes p
        LEFT JOIN (
            SELECT DISTINCT match_id, team_id, player_id, player
            FROM fact_events
            WHERE player_id IS NOT NULL
        ) e ON p.match_id = e.match_id AND p.team_id = e.team_id AND p.player_id = e.player_id
        WHERE p.is_completed = TRUE
        GROUP BY p.match_id, p.team_id, p.player_id, e.player
    """)
    count = con.execute("SELECT COUNT(*) FROM gold_pass_network_nodes").fetchone()[0]
    log.info(f"  gold_pass_network_nodes: {count} rows")


def build_gold_pass_network_edges(con) -> None:
    _exec(con, "DROP TABLE IF EXISTS gold_pass_network_edges")
    _exec(con, """
        CREATE TABLE gold_pass_network_edges AS
        SELECT
            p.match_id,
            p.team_id,
            p.player_id AS passer_id,
            p.recipient_id,
            ep.player AS passer_name,
            er.player AS recipient_name,
            COUNT(*) AS pass_count,
            AVG(p.location_x) AS avg_start_x,
            AVG(p.location_y) AS avg_start_y,
            AVG(p.end_location_x) AS avg_end_x,
            AVG(p.end_location_y) AS avg_end_y
        FROM fact_passes p
        LEFT JOIN (
            SELECT DISTINCT match_id, team_id, player_id, player
            FROM fact_events WHERE player_id IS NOT NULL
        ) ep ON p.match_id = ep.match_id AND p.team_id = ep.team_id AND p.player_id = ep.player_id
        LEFT JOIN (
            SELECT DISTINCT match_id, player_id, player
            FROM fact_events WHERE player_id IS NOT NULL
        ) er ON p.match_id = er.match_id AND p.recipient_id = er.player_id
        WHERE p.is_completed = TRUE AND p.recipient_id IS NOT NULL
        GROUP BY p.match_id, p.team_id, p.player_id, p.recipient_id, ep.player, er.player
        HAVING COUNT(*) >= 2
    """)
    count = con.execute("SELECT COUNT(*) FROM gold_pass_network_edges").fetchone()[0]
    log.info(f"  gold_pass_network_edges: {count} rows")


def build_gold_shot_map(con) -> None:
    _exec(con, "DROP TABLE IF EXISTS gold_shot_map")
    _exec(con, """
        CREATE TABLE gold_shot_map AS
        SELECT
            s.match_id,
            s.team_id,
            e.team AS team_name,
            ep.player AS player_name,
            s.location_x,
            s.location_y,
            s.end_location_x,
            s.end_location_y,
            s.xg,
            s.outcome,
            s.is_goal,
            s.body_part,
            s.shot_type,
            s.technique,
            s.period,
            s.minute,
            s.second
        FROM fact_shots s
        LEFT JOIN (
            SELECT DISTINCT match_id, team_id, team
            FROM fact_events
        ) e ON s.match_id = e.match_id AND s.team_id = e.team_id
        LEFT JOIN (
            SELECT DISTINCT match_id, player_id, player
            FROM fact_events WHERE player_id IS NOT NULL
        ) ep ON s.match_id = ep.match_id AND s.player_id = ep.player_id
    """)
    count = con.execute("SELECT COUNT(*) FROM gold_shot_map").fetchone()[0]
    log.info(f"  gold_shot_map: {count} rows")


def build_gold_formation_positions(con) -> None:
    _exec(con, "DROP TABLE IF EXISTS gold_formation_positions")
    _exec(con, """
        CREATE TABLE gold_formation_positions AS
        WITH touches AS (
            SELECT match_id, team_id, player_id,
                   AVG(location_x) AS avg_x,
                   AVG(location_y) AS avg_y,
                   COUNT(*) AS touch_count
            FROM fact_passes
            WHERE location_x IS NOT NULL AND location_y IS NOT NULL
            GROUP BY match_id, team_id, player_id
            UNION ALL
            SELECT match_id, team_id, player_id,
                   AVG(location_x) AS avg_x,
                   AVG(location_y) AS avg_y,
                   COUNT(*) AS touch_count
            FROM fact_carries
            WHERE location_x IS NOT NULL AND location_y IS NOT NULL
            GROUP BY match_id, team_id, player_id
        ),
        agg AS (
            SELECT match_id, team_id, player_id,
                   SUM(avg_x * touch_count) / SUM(touch_count) AS avg_x,
                   SUM(avg_y * touch_count) / SUM(touch_count) AS avg_y,
                   SUM(touch_count) AS touch_count
            FROM touches
            GROUP BY match_id, team_id, player_id
        )
        SELECT
            a.match_id,
            a.team_id,
            e.team AS team_name,
            ep.player AS player_name,
            a.player_id,
            l.jersey_number,
            l.position,
            a.avg_x,
            a.avg_y,
            a.touch_count
        FROM agg a
        LEFT JOIN (
            SELECT DISTINCT match_id, team_id, team
            FROM fact_events
        ) e ON a.match_id = e.match_id AND a.team_id = e.team_id
        LEFT JOIN (
            SELECT DISTINCT match_id, player_id, player
            FROM fact_events WHERE player_id IS NOT NULL
        ) ep ON a.match_id = ep.match_id AND a.player_id = ep.player_id
        LEFT JOIN fact_lineups l
            ON a.match_id = l.match_id AND a.player_id = l.player_id
    """)
    count = con.execute("SELECT COUNT(*) FROM gold_formation_positions").fetchone()[0]
    log.info(f"  gold_formation_positions: {count} rows")


def build_gold_team_stats(con) -> None:
    _exec(con, "DROP TABLE IF EXISTS gold_team_stats")
    _exec(con, """
        CREATE TABLE gold_team_stats AS
        SELECT
            s.match_id,
            s.team_id,
            e.team AS team_name,
            COUNT(*) AS total_shots,
            SUM(CASE WHEN s.outcome IN ('Saved', 'Goal') THEN 1 ELSE 0 END) AS shots_on_target,
            SUM(CASE WHEN s.is_goal THEN 1 ELSE 0 END) AS goals,
            ROUND(SUM(s.xg), 3) AS total_xg,
            (SELECT COUNT(*) FROM fact_passes p
             WHERE p.match_id = s.match_id AND p.team_id = s.team_id) AS total_passes,
            (SELECT ROUND(
                100.0 * SUM(CASE WHEN p.is_completed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
                1)
             FROM fact_passes p
             WHERE p.match_id = s.match_id AND p.team_id = s.team_id) AS pass_completion_pct,
            (SELECT COUNT(*) FROM fact_carries c
             WHERE c.match_id = s.match_id AND c.team_id = s.team_id) AS total_carries,
            (SELECT COUNT(*) FROM fact_events ev
             WHERE ev.match_id = s.match_id AND ev.team_id = s.team_id AND ev.type = 'Pressure') AS total_pressures
        FROM fact_shots s
        LEFT JOIN (
            SELECT DISTINCT match_id, team_id, team
            FROM fact_events
        ) e ON s.match_id = e.match_id AND s.team_id = e.team_id
        GROUP BY s.match_id, s.team_id, e.team
    """)
    count = con.execute("SELECT COUNT(*) FROM gold_team_stats").fetchone()[0]
    log.info(f"  gold_team_stats: {count} rows")


def run() -> None:
    log.info("Building gold layer …")
    con = get_db_connection(read_only=False)
    try:
        build_gold_xg_timeline(con)
        build_gold_pass_network_nodes(con)
        build_gold_pass_network_edges(con)
        build_gold_shot_map(con)
        build_gold_formation_positions(con)
        build_gold_team_stats(con)
        log.info("Gold layer complete.")
    finally:
        con.close()


if __name__ == "__main__":
    run()
