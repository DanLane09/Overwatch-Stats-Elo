from fastapi import FastAPI
import psycopg2
from typing import Optional
from datetime import datetime, date, time, timezone, timedelta

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="experiment_ow_stats_elo",
    user="postgres",
    password="pass"
)
cur = conn.cursor()
app = FastAPI()


def date_to_utc_range(d: date):
    start = datetime.combine(d, time.min).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


@app.get("/")
def read_root():
    return {"message": "Hello World"}


@app.get("/player-map-winrate/{player_id}")
def player_map_winrate(
    player_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    map_id: Optional[int] = None,
    tournament_id: Optional[int] = None,
    opponent_id: Optional[int] = None,
):
    query = """
        SELECT
            COUNT(*) FILTER (WHERE pms.team_id = mp.winning_team_id) AS maps_won,
            COUNT(*) FILTER (WHERE pms.team_id <> mp.winning_team_id) AS maps_lost
        FROM player_map_stats pms
        JOIN maps_played mp
            ON pms.map_played_id = mp.map_played_id
        JOIN matches m
            ON mp.match_id = m.match_id
        WHERE pms.player_id = %s
    """

    params = [player_id]

    if start_date:
        start_dt, _ = date_to_utc_range(start_date)
        query += " AND m.date_played >= %s"
        params.append(start_dt)

    if end_date:
        _, end_dt = date_to_utc_range(end_date)
        query += " AND m.date_played < %s"
        params.append(end_dt)

    if map_id:
        query += " AND mp.map_id = %s"
        params.append(map_id)

    if tournament_id:
        query += " AND m.tournament_id = %s"
        params.append(tournament_id)

    if opponent_id:
        query += " AND pms.opponent_id = %s"
        params.append(opponent_id)

    cur.execute(query, params)
    winning_count, losing_count = cur.fetchone()

    total = winning_count + losing_count
    win_rate = (winning_count / total * 100) if total > 0 else 0

    return {
        "id": player_id,
        "maps_won": winning_count,
        "maps_lost": losing_count,
        "win_rate": f"{win_rate:.2f}%"
    }


@app.get("/hero-fight-winrate/{hero_id}")
def hero_fight_winrate (
    hero_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    map_id: Optional[int] = None,
    tournament_id: Optional[int] = None,
    team_id: Optional[int] = None,
    opponent_id: Optional[int] = None,
):
    query = """
        WITH hero_fight_presence AS (
    SELECT DISTINCT
        f.id AS fight_id,
        f.winning_team_id,
        pms.team_id,
        pms.opponent_id,
        pms.map_played_id
    FROM fact_team_fight_events f
    JOIN player_timeseries pts
        ON pts.map_played_id = f.map_played_id
        AND pts.time_seconds BETWEEN f.start_time AND f.end_time
    JOIN player_map_stats pms
        ON pms.map_played_id = f.map_played_id
        AND pms.player_id = pts.player_id
    WHERE pts.hero_id = %s
    )

    SELECT
        COUNT(*) FILTER (WHERE hfp.team_id = hfp.winning_team_id) AS fights_won,
        COUNT(*) AS fights_total
    FROM hero_fight_presence as hfp
    JOIN maps_played mp
        ON hfp.map_played_id = mp.map_played_id
    JOIN matches m
        ON mp.match_id = m.match_id 
    """
    params = [hero_id]

    cur.execute(query, params)
    winning_count, losing_count = cur.fetchone()

    total = winning_count + losing_count
    win_rate = (winning_count / total * 100) if total > 0 else 0

    return {
        "id": hero_id,
        "fights_won": winning_count,
        "fights_lost": losing_count,
        "win_rate": f"{win_rate:.2f}%"
    }