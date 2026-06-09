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


def add_start_date(start_date: date, query: str, params: list) -> tuple:
    if start_date:
        start_dt, _ = date_to_utc_range(start_date)
        query += " AND m.date_played >= %s"
        params.append(start_dt)
    return query, params


def add_end_date(end_date: date, query: str, params: list) -> tuple:
    if end_date:
        _, end_dt = date_to_utc_range(end_date)
        query += " AND m.date_played < %s"
        params.append(end_dt)
    return query, params


def add_map_id(map_id: int, query: str, params: list) -> tuple:
    if map_id:
        query += " AND mp.map_id = %s"
        params.append(map_id)
    return query, params


def add_map_played_id(map_played_id: int, query: str, params: list) -> tuple:
    if map_played_id:
        query += " AND mp.map_played_id = %s"
        params.append(map_played_id)
    return query, params


def add_match_id(match_id: int, query: str, params: list) -> tuple:
    if match_id:
        query += " AND m.match_id = %s"
        params.append(match_id)
    return query, params


def add_tournament_id(tournament_id: int, query: str, params: list) -> tuple:
    if tournament_id:
        query += " AND m.tournament_id = %s"
        params.append(tournament_id)
    return query, params


# FIXED: Added table_alias to support different baseline telemetry tables
def add_team_id(team_id: int, query: str, params: list, table_alias: str = "pts") -> tuple:
    if team_id:
        query += f" AND {table_alias}.team_id = %s"
        params.append(team_id)
    return query, params


def add_opponent_id_basic(opponent_id: int, query: str, params: list, table_alias: str = "pms") -> tuple:
    if opponent_id:
        query += f" AND {table_alias}.opponent_id = %s"
        params.append(opponent_id)
    return query, params


# FIXED: Added table_alias to handle alternative context tables cleanly
def add_opponent_id_by_compare(opponent_id: int, query: str, params: list, table_alias: str = "pts") -> tuple:
    if opponent_id:
        query += f" AND ((mp.blue_team_id = {table_alias}.team_id AND mp.red_team_id = %s) OR (mp.red_team_id = {table_alias}.team_id AND mp.blue_team_id = %s))"
        params.extend([opponent_id, opponent_id])
    return query, params


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

    query, params = add_start_date(start_date, query, params)
    query, params = add_end_date(end_date, query, params)
    query, params = add_map_id(map_id, query, params)
    query, params = add_tournament_id(tournament_id, query, params)
    query, params = add_opponent_id_basic(opponent_id, query, params, table_alias="pms")

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


@app.get("/hero-fight-winrate")
def hero_fight_winrate(
        hero_id: Optional[int] = None,
        exclude_mirrors: Optional[bool] = True,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        map_id: Optional[int] = None,
        match_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    stats_query = """
        SELECT 
            pts.hero_id,
            COUNT(*) FILTER (WHERE pts.team_id = tfe.winning_team_id) AS fights_won, 
            COUNT(*) FILTER (WHERE pts.team_id <> tfe.winning_team_id) AS fights_lost
        FROM player_timeseries pts 
        JOIN fact_team_fight_events tfe 
            ON pts.time_seconds = tfe.start_time 
            AND pts.map_played_id = tfe.map_played_id
        JOIN maps_played mp
            ON pts.map_played_id = mp.map_played_id
        JOIN matches m
            ON mp.match_id = m.match_id
        WHERE 1=1
    """
    params = []

    if exclude_mirrors:
        stats_query += """ AND NOT (
                EXISTS (
                    SELECT 1 FROM player_timeseries mirror
                    WHERE mirror.map_played_id = tfe.map_played_id 
                      AND mirror.time_seconds = tfe.start_time 
                      AND mirror.hero_id = pts.hero_id 
                      AND mirror.team_id = mp.blue_team_id
                ) AND EXISTS (
                    SELECT 1 FROM player_timeseries mirror
                    WHERE mirror.map_played_id = tfe.map_played_id 
                      AND mirror.time_seconds = tfe.start_time 
                      AND mirror.hero_id = pts.hero_id 
                      AND mirror.team_id = mp.red_team_id
                )
            )"""

    stats_query, params = add_start_date(start_date, stats_query, params)
    stats_query, params = add_end_date(end_date, stats_query, params)
    stats_query, params = add_map_played_id(map_played_id, stats_query, params)
    stats_query, params = add_map_id(map_id, stats_query, params)
    stats_query, params = add_match_id(match_id, stats_query, params)
    stats_query, params = add_tournament_id(tournament_id, stats_query, params)

    # Uses default "pts" alias
    stats_query, params = add_team_id(team_id, stats_query, params, table_alias="pts")
    stats_query, params = add_opponent_id_by_compare(opponent_id, stats_query, params, table_alias="pts")

    stats_query += " GROUP BY pts.hero_id"

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        params.append(hero_id)

    final_query = f"""
        WITH filtered_fights AS (
            {stats_query}
        )
        SELECT 
            h.hero_id,
            COALESCE(ff.fights_won, 0) AS fights_won,
            COALESCE(ff.fights_lost, 0) AS fights_lost
        FROM heroes h
        LEFT JOIN filtered_fights ff ON h.hero_id = ff.hero_id
        WHERE 1=1 {hero_filter}
        ORDER BY h.hero_id ASC
    """

    cur.execute(final_query, params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, winning_count, losing_count = row
        total = winning_count + losing_count
        win_rate = (winning_count / total * 100) if total > 0 else 0.0

        response_data.append({
            "id": h_id,
            "fights_won": winning_count,
            "fights_lost": losing_count,
            "win_rate": f"{win_rate:.2f}%"
        })

    return response_data


@app.get("/hero-playtime")
def hero_playtime(
        hero_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        match_id: Optional[int] = None,
        map_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    query = """
        WITH filtered_stats AS (
            SELECT 
                phms.hero_id,
                phms.seconds_played
            FROM player_hero_map_stats phms 
            JOIN maps_played mp ON phms.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id
            WHERE 1=1
            {shared_filters}
        ),
        total_pool AS (
            SELECT COALESCE(SUM(seconds_played), 0) AS total_time 
            FROM filtered_stats
        )
        SELECT 
            h.hero_id,
            COALESCE(SUM(fs.seconds_played), 0) AS hero_time,
            COALESCE(tp.total_time, 0) AS total_time
        FROM heroes h
        LEFT JOIN filtered_stats fs ON h.hero_id = fs.hero_id
        CROSS JOIN total_pool tp
        WHERE 1=1 {hero_filter}
        GROUP BY h.hero_id, tp.total_time
        ORDER BY h.hero_id
    """

    shared_filters = ""
    params = []

    shared_filters, params = add_start_date(start_date, shared_filters, params)
    shared_filters, params = add_end_date(end_date, shared_filters, params)
    shared_filters, params = add_map_played_id(map_played_id, shared_filters, params)
    shared_filters, params = add_map_id(map_id, shared_filters, params)
    shared_filters, params = add_match_id(match_id, shared_filters, params)
    shared_filters, params = add_tournament_id(tournament_id, shared_filters, params)

    # Passing "phms" explicitly fixes the runtime mapping error
    shared_filters, params = add_team_id(team_id, shared_filters, params, table_alias="phms")
    shared_filters, params = add_opponent_id_by_compare(opponent_id, shared_filters, params, table_alias="phms")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        params.append(hero_id)

    formatted_query = query.format(shared_filters=shared_filters, hero_filter=hero_filter)
    cur.execute(formatted_query, params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, hero_time, total_time = row
        available_context_time = total_time / 5
        playtime_percentage = (hero_time / available_context_time * 100) if available_context_time > 0 else 0.0

        response_data.append({
            "hero_id": h_id,
            "hero_playtime_seconds": hero_time,
            "total_available_seconds": available_context_time,
            "playtime_percentage": f"{playtime_percentage:.2f}%"
        })

    return response_data


@app.get("/hero-scoreboard")
def hero_scoreboard(
        hero_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        match_id: Optional[int] = None,
        map_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    base_query = """
        WITH filtered_stats AS (
            SELECT 
                phms.hero_id,
                SUM(phms.eliminations) AS eliminations,
                SUM(phms.assists) AS assists,
                SUM(phms.deaths) AS deaths,
                SUM(phms.damage) AS damage,
                SUM(phms.healing) AS healing,
                SUM(phms.mitigated) AS mitigated,
                SUM(phms.seconds_played) AS seconds_played
            FROM player_hero_map_stats phms
            JOIN maps_played mp ON phms.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id
            WHERE 1 = 1 {filters}
            GROUP BY phms.hero_id
        )
        SELECT 
            h.hero_id,
            COALESCE(fs.eliminations, 0),
            COALESCE(fs.assists, 0),
            COALESCE(fs.deaths, 0),
            COALESCE(fs.damage, 0),
            COALESCE(fs.healing, 0),
            COALESCE(fs.mitigated, 0),
            COALESCE(fs.seconds_played, 0)
        FROM heroes h
        LEFT JOIN filtered_stats fs ON h.hero_id = fs.hero_id
        WHERE 1 = 1 {hero_filter}
        ORDER BY h.hero_id ASC
    """

    response_filter = ""
    params = []

    response_filter, params = add_start_date(start_date, response_filter, params)
    response_filter, params = add_end_date(end_date, response_filter, params)
    response_filter, params = add_map_played_id(map_played_id, response_filter, params)
    response_filter, params = add_map_id(map_id, response_filter, params)
    response_filter, params = add_match_id(match_id, response_filter, params)
    response_filter, params = add_tournament_id(tournament_id, response_filter, params)

    # Passing "phms" explicitly fixes the query composition context
    response_filter, params = add_team_id(team_id, response_filter, params, table_alias="phms")
    response_filter, params = add_opponent_id_by_compare(opponent_id, response_filter, params, table_alias="phms")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        params.append(hero_id)

    formatted_query = base_query.format(filters=response_filter, hero_filter=hero_filter)
    cur.execute(formatted_query, params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, eliminations, assists, deaths, damage, healing, mitigated, time_played = row
        response_data.append({
            "hero_id": h_id,
            "total_eliminations": eliminations,
            "total_assists": assists,
            "total_deaths": deaths,
            "total_damage": damage,
            "total_healing": healing,
            "total_mitigated": mitigated,
            "total_seconds_played": time_played,
        })

    return response_data


@app.get("/hero-ultimate-usage")
def hero_ultimate_usage(
        hero_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        match_id: Optional[int] = None,
        map_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    # FIXED: standardizing the event alias to "ev" inside both SQL chunks
    charge_query = """
        WITH filtered_stats AS (
            SELECT 
                ev.hero_id,
                COUNT(*) AS ults_charged, 
                SUM(ev.charge_duration) AS total_charge_time
            FROM fact_ult_charge_events ev
            JOIN maps_played mp ON ev.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id 
            WHERE 1 = 1 {filters}
            GROUP BY ev.hero_id
        )
        SELECT 
            h.hero_id,
            COALESCE(fs.ults_charged, 0),
            COALESCE(fs.total_charge_time, 0.0)
        FROM heroes h
        LEFT JOIN filtered_stats fs ON h.hero_id = fs.hero_id
        WHERE 1 = 1 {hero_filter}
        ORDER BY h.hero_id ASC
    """

    # FIXED: fixed the broken column selections, typos, and changed table alias to "ev"
    usage_query = """
        WITH filtered_stats AS (
            SELECT 
                ev.hero_id,
                COUNT(*) AS ults_used, 
                SUM(ev.hold_duration) AS total_hold_time
            FROM fact_ult_usage_events ev
            JOIN maps_played mp ON ev.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id 
            WHERE 1 = 1 {filters}
            GROUP BY ev.hero_id
        )
        SELECT 
            h.hero_id,
            COALESCE(fs.ults_used, 0),
            COALESCE(fs.total_hold_time, 0.0)
        FROM heroes h
        LEFT JOIN filtered_stats fs ON h.hero_id = fs.hero_id
        WHERE 1 = 1 {hero_filter}
        ORDER BY h.hero_id ASC
    """

    filter_params = []
    filter_string = ""

    filter_string, filter_params = add_start_date(start_date, filter_string, filter_params)
    filter_string, filter_params = add_end_date(end_date, filter_string, filter_params)
    filter_string, filter_params = add_map_played_id(map_played_id, filter_string, filter_params)
    filter_string, filter_params = add_map_id(map_id, filter_string, filter_params)
    filter_string, filter_params = add_match_id(match_id, filter_string, filter_params)
    filter_string, filter_params = add_tournament_id(tournament_id, filter_string, filter_params)

    # FIXED: Passing unified "ev" alias so it targets both queries gracefully
    filter_string, filter_params = add_team_id(team_id, filter_string, filter_params, table_alias="ev")
    filter_string, filter_params = add_opponent_id_by_compare(opponent_id, filter_string, filter_params,
                                                              table_alias="ev")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        filter_params.append(hero_id)

    final_charge_query = charge_query.format(filters=filter_string, hero_filter=hero_filter)
    cur.execute(final_charge_query, filter_params)
    charge_results = cur.fetchall()

    final_usage_query = usage_query.format(filters=filter_string, hero_filter=hero_filter)
    cur.execute(final_usage_query, filter_params)
    usage_results = cur.fetchall()

    heroes_data = {}

    for row in charge_results:
        h_id, ult_charges, total_charge_dur = row
        heroes_data[h_id] = {
            "id": h_id,
            "ult_charges_built": ult_charges,
            "total_charge_duration_seconds": float(total_charge_dur),
            "ult_usages": 0,
            "total_hold_duration_seconds": 0.0
        }

    for row in usage_results:
        h_id, ult_usages, total_hold_dur = row
        if h_id not in heroes_data:
            heroes_data[h_id] = {
                "id": h_id,
                "ult_charges_built": 0,
                "total_charge_duration_seconds": 0.0,
            }
        heroes_data[h_id]["ult_usages"] = ult_usages
        heroes_data[h_id]["total_hold_duration_seconds"] = float(total_hold_dur)

    response_data = []
    for h_id, data in heroes_data.items():
        charges = data["ult_charges_built"]
        usages = data["ult_usages"]
        charge_dur = data["total_charge_duration_seconds"]
        hold_dur = data["total_hold_duration_seconds"]

        data["avg_seconds_to_charge"] = round(charge_dur / charges, 2) if charges > 0 else 0.0
        data["avg_seconds_held_before_use"] = round(hold_dur / usages, 2) if usages > 0 else 0.0

        response_data.append(data)

    response_data.sort(key=lambda x: x["id"])

    return response_data