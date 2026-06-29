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
        JOIN maps_played mp ON pts.map_played_id = mp.map_played_id
        JOIN matches m ON mp.match_id = m.match_id
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
    stats_query, params = add_team_id(team_id, stats_query, params, table_alias="pts")
    stats_query, params = add_opponent_id_by_compare(opponent_id, stats_query, params, table_alias="pts")
    stats_query += " GROUP BY pts.hero_id"

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        params.append(hero_id)

    final_query = f"""
        WITH filtered_fights AS ( {stats_query} )
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
    general_results = cur.fetchall()


    ult_stats_query = """
        SELECT 
            uue.hero_id,
            COUNT(*) FILTER (WHERE pms.team_id = tfe.winning_team_id) AS ult_fights_won, 
            COUNT(*) FILTER (WHERE pms.team_id <> tfe.winning_team_id) AS ult_fights_lost
        FROM fact_ult_usage_events uue
        JOIN fact_team_fight_events tfe 
            ON uue.map_played_id = tfe.map_played_id
            AND uue.ult_used_time >= tfe.start_time 
            AND uue.ult_used_time <= tfe.end_time
        JOIN player_map_stats pms 
            ON uue.player_id = pms.player_id 
            AND uue.map_played_id = pms.map_played_id
        JOIN maps_played mp ON uue.map_played_id = mp.map_played_id
        JOIN matches m ON mp.match_id = m.match_id
        WHERE 1=1
    """
    ult_params = []

    if exclude_mirrors:
        ult_stats_query += """ AND NOT (
                EXISTS (
                    SELECT 1 FROM player_timeseries mirror
                    WHERE mirror.map_played_id = tfe.map_played_id 
                      AND mirror.time_seconds = tfe.start_time 
                      AND mirror.hero_id = uue.hero_id 
                      AND mirror.team_id = mp.blue_team_id
                ) AND EXISTS (
                    SELECT 1 FROM player_timeseries mirror
                    WHERE mirror.map_played_id = tfe.map_played_id 
                      AND mirror.time_seconds = tfe.start_time 
                      AND mirror.hero_id = uue.hero_id 
                      AND mirror.team_id = mp.red_team_id
                )
            )"""

    ult_stats_query, ult_params = add_start_date(start_date, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_end_date(end_date, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_map_played_id(map_played_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_map_id(map_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_match_id(match_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_tournament_id(tournament_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_team_id(team_id, ult_stats_query, ult_params, table_alias="pms")
    ult_stats_query, ult_params = add_opponent_id_by_compare(opponent_id, ult_stats_query, ult_params, table_alias="pms")
    ult_stats_query += " GROUP BY uue.hero_id"

    ult_hero_filter = ""
    if hero_id:
        ult_hero_filter = " AND h.hero_id = %s"
        ult_params.append(hero_id)

    final_ult_query = f"""
        WITH filtered_ult_fights AS ( {ult_stats_query} )
        SELECT 
            h.hero_id,
            COALESCE(uf.ult_fights_won, 0) AS ult_fights_won,
            COALESCE(uf.ult_fights_lost, 0) AS ult_fights_lost
        FROM heroes h
        LEFT JOIN filtered_ult_fights uf ON h.hero_id = uf.hero_id
        WHERE 1=1 {ult_hero_filter}
        ORDER BY h.hero_id ASC
    """
    cur.execute(final_ult_query, ult_params)
    ult_results = cur.fetchall()



    heroes_data = {}

    for row in general_results:
        h_id, winning_count, losing_count = row
        total = winning_count + losing_count
        win_rate = (winning_count / total * 100) if total > 0 else 0.0

        heroes_data[h_id] = {
            "id": h_id,
            "fights_won": winning_count,
            "fights_lost": losing_count,
            "win_rate": f"{win_rate:.2f}%",
            "ult_fights_won": 0,
            "ult_fights_lost": 0,
            "ult_efficiency": "0.00%"
        }

    for row in ult_results:
        h_id, ult_won, ult_lost = row
        if h_id in heroes_data:
            ult_total = ult_won + ult_lost
            ult_efficiency = (ult_won / ult_total * 100) if ult_total > 0 else 0.0

            heroes_data[h_id]["ult_fights_won"] = ult_won
            heroes_data[h_id]["ult_fights_lost"] = ult_lost
            heroes_data[h_id]["ult_efficiency"] = f"{ult_efficiency:.2f}%"

    return list(heroes_data.values())


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

@app.get("/hero-ultimate-situations")
def hero_ultimate_situations(
        hero_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        map_id: Optional[int] = None,
        match_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    base_query = """
        WITH player_deaths AS (
            -- Calculate individual death events by finding where the death count increments
            SELECT 
                map_played_id,
                player_id,
                team_id,
                time_seconds AS death_time
            FROM player_timeseries
            WHERE deaths_delta > 0
        ),
        ult_situations AS (
            -- For each ultimate used, calculate how many teammates and enemies are currently dead (within last 15s)
            SELECT 
                uue.hero_id,
                (
                    SELECT COUNT(DISTINCT pd.player_id) 
                    FROM player_deaths pd 
                    WHERE pd.map_played_id = uue.map_played_id 
                      AND pd.team_id = pms.team_id
                      AND pd.death_time >= uue.ult_used_time - 15 
                      AND pd.death_time <= uue.ult_used_time
                ) AS dead_allies,
                (
                    SELECT COUNT(DISTINCT pd.player_id) 
                    FROM player_deaths pd 
                    WHERE pd.map_played_id = uue.map_played_id 
                      AND pd.team_id = pms.opponent_id
                      AND pd.death_time >= uue.ult_used_time - 15 
                      AND pd.death_time <= uue.ult_used_time
                ) AS dead_enemies
            FROM fact_ult_usage_events uue
            JOIN player_map_stats pms 
                ON uue.player_id = pms.player_id 
                AND uue.map_played_id = pms.map_played_id
            JOIN maps_played mp ON uue.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id
            WHERE 1 = 1 {filters}
        ),
        aggregated_stats AS (
            -- Classify each ult scenario into even, player up (advantage), or player down (disadvantage)
            SELECT 
                hero_id,
                COUNT(*) FILTER (WHERE dead_allies = dead_enemies) AS ults_even,
                COUNT(*) FILTER (WHERE dead_allies < dead_enemies) AS ults_advantage,
                COUNT(*) FILTER (WHERE dead_allies > dead_enemies) AS ults_disadvantage
            FROM ult_situations
            GROUP BY hero_id
        )
        SELECT 
            h.hero_id,
            COALESCE(a.ults_even, 0) AS ults_even,
            COALESCE(a.ults_advantage, 0) AS ults_advantage,
            COALESCE(a.ults_disadvantage, 0) AS ults_disadvantage
        FROM heroes h
        LEFT JOIN aggregated_stats a ON h.hero_id = a.hero_id
        WHERE 1 = 1 {hero_filter}
        ORDER BY h.hero_id
    """

    filter_params = []
    filter_string = ""

    filter_string, filter_params = add_start_date(start_date, filter_string, filter_params)
    filter_string, filter_params = add_end_date(end_date, filter_string, filter_params)
    filter_string, filter_params = add_map_played_id(map_played_id, filter_string, filter_params)
    filter_string, filter_params = add_map_id(map_id, filter_string, filter_params)
    filter_string, filter_params = add_match_id(match_id, filter_string, filter_params)
    filter_string, filter_params = add_tournament_id(tournament_id, filter_string, filter_params)
    filter_string, filter_params = add_team_id(team_id, filter_string, filter_params, table_alias="pms")
    filter_string, filter_params = add_opponent_id_by_compare(opponent_id, filter_string, filter_params, table_alias="pms")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        filter_params.append(hero_id)

    formatted_query = base_query.format(filters=filter_string, hero_filter=hero_filter)
    cur.execute(formatted_query, filter_params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, even, up_player, down_player = row
        total_ults = even + up_player + down_player

        response_data.append({
            "hero_id": h_id,
            "total_ult_usages": total_ults,
            "situations": {
                "even_fights": even,
                "advantage_fights_player_up": up_player,
                "disadvantage_fights_player_down": down_player
            }
        })

    return response_data


@app.get("/hero-first-death-rate")
def hero_first_death_rate(
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
    base_query = """
        WITH filtered_first_deaths AS (
            SELECT 
                pts.hero_id,
                COUNT(*) AS total_fights,
                COUNT(*) FILTER (WHERE pts.player_id = tfe.first_death_player_id) AS first_deaths
            FROM player_timeseries pts 
            JOIN fact_team_fight_events tfe 
                ON pts.time_seconds = tfe.start_time 
                AND pts.map_played_id = tfe.map_played_id
            JOIN maps_played mp ON pts.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id
            WHERE 1 = 1 {filters}
            GROUP BY pts.hero_id
        )
        SELECT 
            h.hero_id,
            COALESCE(ffd.total_fights, 0),
            COALESCE(ffd.first_deaths, 0)
        FROM heroes h
        LEFT JOIN filtered_first_deaths ffd ON h.hero_id = ffd.hero_id
        WHERE 1 = 1 {hero_filter}
        ORDER BY h.hero_id ASC
    """

    response_filter = ""
    params = []

    if exclude_mirrors:
        response_filter += """ AND NOT (
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

    response_filter, params = add_start_date(start_date, response_filter, params)
    response_filter, params = add_end_date(end_date, response_filter, params)
    response_filter, params = add_map_played_id(map_played_id, response_filter, params)
    response_filter, params = add_map_id(map_id, response_filter, params)
    response_filter, params = add_match_id(match_id, response_filter, params)
    response_filter, params = add_tournament_id(tournament_id, response_filter, params)
    response_filter, params = add_team_id(team_id, response_filter, params, table_alias="pts")
    response_filter, params = add_opponent_id_by_compare(opponent_id, response_filter, params, table_alias="pts")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND h.hero_id = %s"
        params.append(hero_id)

    formatted_query = base_query.format(filters=response_filter, hero_filter=hero_filter)
    cur.execute(formatted_query, params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, total_fights, first_deaths = row
        first_death_rate = (first_deaths / total_fights * 100) if total_fights > 0 else 0.0

        response_data.append({
            "hero_id": h_id,
            "total_fights_participated": total_fights,
            "first_deaths_count": first_deaths,
            "first_death_rate": f"{first_death_rate:.2f}%"
        })

    return response_data




@app.get("/player/{player_id}/hero-fight-winrate")
def player_hero_fight_winrate(
        player_id: int,
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
        JOIN maps_played mp ON pts.map_played_id = mp.map_played_id
        JOIN matches m ON mp.match_id = m.match_id
        WHERE pts.player_id = %s
    """
    params = [player_id]

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
    stats_query, params = add_team_id(team_id, stats_query, params, table_alias="pts")
    stats_query, params = add_opponent_id_by_compare(opponent_id, stats_query, params, table_alias="pts")
    stats_query += " GROUP BY pts.hero_id"

    hero_filter = ""
    if hero_id:
        hero_filter = " AND ff.hero_id = %s"
        params.append(hero_id)

    final_query = f"""
        WITH filtered_fights AS ( {stats_query} )
        SELECT 
            ff.hero_id,
            COALESCE(ff.fights_won, 0) AS fights_won,
            COALESCE(ff.fights_lost, 0) AS fights_lost
        FROM filtered_fights ff
        WHERE 1=1 {hero_filter}
        ORDER BY ff.hero_id ASC
    """
    cur.execute(final_query, params)
    general_results = cur.fetchall()

    ult_stats_query = """
        SELECT 
            uue.hero_id,
            COUNT(*) FILTER (WHERE pms.team_id = tfe.winning_team_id) AS ult_fights_won, 
            COUNT(*) FILTER (WHERE pms.team_id <> tfe.winning_team_id) AS ult_fights_lost
        FROM fact_ult_usage_events uue
        JOIN fact_team_fight_events tfe 
            ON uue.map_played_id = tfe.map_played_id
            AND uue.ult_used_time >= tfe.start_time 
            AND uue.ult_used_time <= tfe.end_time
        JOIN player_map_stats pms 
            ON uue.player_id = pms.player_id 
            AND uue.map_played_id = pms.map_played_id
        JOIN maps_played mp ON uue.map_played_id = mp.map_played_id
        JOIN matches m ON mp.match_id = m.match_id
        WHERE uue.player_id = %s
    """
    ult_params = [player_id]

    if exclude_mirrors:
        ult_stats_query += """ AND NOT (
                EXISTS (
                    SELECT 1 FROM player_timeseries mirror
                    WHERE mirror.map_played_id = tfe.map_played_id 
                      AND mirror.time_seconds = tfe.start_time 
                      AND mirror.hero_id = uue.hero_id 
                      AND mirror.team_id = mp.blue_team_id
                ) AND EXISTS (
                    SELECT 1 FROM player_timeseries mirror
                    WHERE mirror.map_played_id = tfe.map_played_id 
                      AND mirror.time_seconds = tfe.start_time 
                      AND mirror.hero_id = uue.hero_id 
                      AND mirror.team_id = mp.red_team_id
                )
            )"""

    ult_stats_query, ult_params = add_start_date(start_date, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_end_date(end_date, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_map_played_id(map_played_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_map_id(map_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_match_id(match_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_tournament_id(tournament_id, ult_stats_query, ult_params)
    ult_stats_query, ult_params = add_team_id(team_id, ult_stats_query, ult_params, table_alias="pms")
    ult_stats_query, ult_params = add_opponent_id_by_compare(opponent_id, ult_stats_query, ult_params, table_alias="pms")
    ult_stats_query += " GROUP BY uue.hero_id"

    ult_hero_filter = ""
    if hero_id:
        ult_hero_filter = " AND uf.hero_id = %s"
        ult_params.append(hero_id)

    final_ult_query = f"""
        WITH filtered_ult_fights AS ( {ult_stats_query} )
        SELECT 
            uf.hero_id,
            COALESCE(uf.ult_fights_won, 0) AS ult_fights_won,
            COALESCE(uf.ult_fights_lost, 0) AS ult_fights_lost
        FROM filtered_ult_fights uf
        WHERE 1=1 {ult_hero_filter}
        ORDER BY uf.hero_id ASC
    """
    cur.execute(final_ult_query, ult_params)
    ult_results = cur.fetchall()

    heroes_data = {}
    for row in general_results:
        h_id, winning_count, losing_count = row
        total = winning_count + losing_count
        win_rate = (winning_count / total * 100) if total > 0 else 0.0

        heroes_data[h_id] = {
            "id": h_id,
            "fights_won": winning_count,
            "fights_lost": losing_count,
            "win_rate": f"{win_rate:.2f}%",
            "ult_fights_won": 0,
            "ult_fights_lost": 0,
            "ult_efficiency": "0.00%"
        }

    for row in ult_results:
        h_id, ult_won, ult_lost = row
        if h_id in heroes_data:
            ult_total = ult_won + ult_lost
            ult_efficiency = (ult_won / ult_total * 100) if ult_total > 0 else 0.0

            heroes_data[h_id]["ult_fights_won"] = ult_won
            heroes_data[h_id]["ult_fights_lost"] = ult_lost
            heroes_data[h_id]["ult_efficiency"] = f"{ult_efficiency:.2f}%"

    return list(heroes_data.values())


@app.get("/player/{player_id}/hero-playtime")
def player_hero_playtime(
        player_id: int,
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
            WHERE phms.player_id = %s {shared_filters}
        ),
        total_pool AS (
            SELECT COALESCE(SUM(seconds_played), 0) AS total_time 
            FROM filtered_stats
        )
        SELECT 
            fs.hero_id,
            COALESCE(SUM(fs.seconds_played), 0) AS hero_time,
            COALESCE(tp.total_time, 0) AS total_time
        FROM filtered_stats fs
        CROSS JOIN total_pool tp
        WHERE 1=1 {hero_filter}
        GROUP BY fs.hero_id, tp.total_time
        ORDER BY fs.hero_id
    """

    shared_filters = ""
    params = [player_id]

    shared_filters, params = add_start_date(start_date, shared_filters, params)
    shared_filters, params = add_end_date(end_date, shared_filters, params)
    shared_filters, params = add_map_played_id(map_played_id, shared_filters, params)
    shared_filters, params = add_map_id(map_id, shared_filters, params)
    shared_filters, params = add_match_id(match_id, shared_filters, params)
    shared_filters, params = add_tournament_id(tournament_id, shared_filters, params)
    shared_filters, params = add_team_id(team_id, shared_filters, params, table_alias="phms")
    shared_filters, params = add_opponent_id_by_compare(opponent_id, shared_filters, params, table_alias="phms")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND fs.hero_id = %s"
        params.append(hero_id)

    formatted_query = query.format(shared_filters=shared_filters, hero_filter=hero_filter)
    cur.execute(formatted_query, params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, hero_time, total_time = row
        playtime_percentage = (hero_time / total_time * 100) if total_time > 0 else 0.0

        response_data.append({
            "hero_id": h_id,
            "hero_playtime_seconds": hero_time,
            "total_available_seconds": total_time,
            "playtime_percentage": f"{playtime_percentage:.2f}%"
        })

    return response_data


@app.get("/player/{player_id}/hero-scoreboard")
def player_hero_scoreboard(
        player_id: int,
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
            WHERE phms.player_id = %s {filters}
            GROUP BY phms.hero_id
        )
        SELECT 
            fs.hero_id,
            fs.eliminations,
            fs.assists,
            fs.deaths,
            fs.damage,
            fs.healing,
            fs.mitigated,
            fs.seconds_played
        FROM filtered_stats fs
        WHERE 1 = 1 {hero_filter}
        ORDER BY fs.hero_id ASC
    """

    response_filter = ""
    params = [player_id]

    response_filter, params = add_start_date(start_date, response_filter, params)
    response_filter, params = add_end_date(end_date, response_filter, params)
    response_filter, params = add_map_played_id(map_played_id, response_filter, params)
    response_filter, params = add_map_id(map_id, response_filter, params)
    response_filter, params = add_match_id(match_id, response_filter, params)
    response_filter, params = add_tournament_id(tournament_id, response_filter, params)
    response_filter, params = add_team_id(team_id, response_filter, params, table_alias="phms")
    response_filter, params = add_opponent_id_by_compare(opponent_id, response_filter, params, table_alias="phms")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND fs.hero_id = %s"
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


@app.get("/player/{player_id}/hero-ultimate-usage")
def player_hero_ultimate_usage(
        player_id: int,
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
    charge_query = """
        WITH filtered_stats AS (
            SELECT 
                ev.hero_id,
                COUNT(*) AS ults_charged, 
                SUM(ev.charge_duration) AS total_charge_time
            FROM fact_ult_charge_events ev
            JOIN maps_played mp ON ev.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id 
            WHERE ev.player_id = %s {filters}
            GROUP BY ev.hero_id
        )
        SELECT 
            fs.hero_id,
            fs.ults_charged,
            fs.total_charge_time
        FROM filtered_stats fs
        WHERE 1 = 1 {hero_filter}
        ORDER BY fs.hero_id ASC
    """

    usage_query = """
        WITH filtered_stats AS (
            SELECT 
                ev.hero_id,
                COUNT(*) AS ults_used, 
                SUM(ev.hold_duration) AS total_hold_time
            FROM fact_ult_usage_events ev
            JOIN maps_played mp ON ev.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id 
            WHERE ev.player_id = %s {filters}
            GROUP BY ev.hero_id
        )
        SELECT 
            fs.hero_id,
            fs.ults_used,
            fs.total_hold_time
        FROM filtered_stats fs
        WHERE 1 = 1 {hero_filter}
        ORDER BY fs.hero_id ASC
    """

    filter_params = [player_id]
    filter_string = ""

    filter_string, filter_params = add_start_date(start_date, filter_string, filter_params)
    filter_string, filter_params = add_end_date(end_date, filter_string, filter_params)
    filter_string, filter_params = add_map_played_id(map_played_id, filter_string, filter_params)
    filter_string, filter_params = add_map_id(map_id, filter_string, filter_params)
    filter_string, filter_params = add_match_id(match_id, filter_string, filter_params)
    filter_string, filter_params = add_tournament_id(tournament_id, filter_string, filter_params)
    filter_string, filter_params = add_team_id(team_id, filter_string, filter_params, table_alias="ev")
    filter_string, filter_params = add_opponent_id_by_compare(opponent_id, filter_string, filter_params, table_alias="ev")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND fs.hero_id = %s"
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


@app.get("/player/{player_id}/hero-ultimate-situations")
def player_hero_ultimate_situations(
        player_id: int,
        hero_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        map_id: Optional[int] = None,
        match_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    base_query = """
        WITH player_deaths AS (
            SELECT 
                map_played_id,
                player_id,
                team_id,
                time_seconds AS death_time
            FROM player_timeseries
            WHERE deaths_delta > 0
        ),
        ult_situations AS (
            SELECT 
                uue.hero_id,
                (
                    SELECT COUNT(DISTINCT pd.player_id) 
                    FROM player_deaths pd 
                    WHERE pd.map_played_id = uue.map_played_id 
                      AND pd.team_id = pms.team_id
                      AND pd.death_time >= uue.ult_used_time - 15 
                      AND pd.death_time <= uue.ult_used_time
                ) AS dead_allies,
                (
                    SELECT COUNT(DISTINCT pd.player_id) 
                    FROM player_deaths pd 
                    WHERE pd.map_played_id = uue.map_played_id 
                      AND pd.team_id = pms.opponent_id
                      AND pd.death_time >= uue.ult_used_time - 15 
                      AND pd.death_time <= uue.ult_used_time
                ) AS dead_enemies
            FROM fact_ult_usage_events uue
            JOIN player_map_stats pms 
                ON uue.player_id = pms.player_id 
                AND uue.map_played_id = pms.map_played_id
            JOIN maps_played mp ON uue.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id
            WHERE uue.player_id = %s {filters}
        ),
        aggregated_stats AS (
            SELECT 
                hero_id,
                COUNT(*) FILTER (WHERE dead_allies = dead_enemies) AS ults_even,
                COUNT(*) FILTER (WHERE dead_allies < dead_enemies) AS ults_advantage,
                COUNT(*) FILTER (WHERE dead_allies > dead_enemies) AS ults_disadvantage
            FROM ult_situations
            GROUP BY hero_id
        )
        SELECT 
            a.hero_id,
            COALESCE(a.ults_even, 0) AS ults_even,
            COALESCE(a.ults_advantage, 0) AS ults_advantage,
            COALESCE(a.ults_disadvantage, 0) AS ults_disadvantage
        FROM aggregated_stats a
        WHERE 1 = 1 {hero_filter}
        ORDER BY a.hero_id
    """

    filter_params = [player_id]
    filter_string = ""

    filter_string, filter_params = add_start_date(start_date, filter_string, filter_params)
    filter_string, filter_params = add_end_date(end_date, filter_string, filter_params)
    filter_string, filter_params = add_map_played_id(map_played_id, filter_string, filter_params)
    filter_string, filter_params = add_map_id(map_id, filter_string, filter_params)
    filter_string, filter_params = add_match_id(match_id, filter_string, filter_params)
    filter_string, filter_params = add_tournament_id(tournament_id, filter_string, filter_params)
    filter_string, filter_params = add_team_id(team_id, filter_string, filter_params, table_alias="pms")
    filter_string, filter_params = add_opponent_id_by_compare(opponent_id, filter_string, filter_params, table_alias="pms")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND a.hero_id = %s"
        filter_params.append(hero_id)

    formatted_query = base_query.format(filters=filter_string, hero_filter=hero_filter)
    cur.execute(formatted_query, filter_params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, even, up_player, down_player = row
        total_ults = even + up_player + down_player

        response_data.append({
            "hero_id": h_id,
            "total_ult_usages": total_ults,
            "situations": {
                "even_fights": even,
                "advantage_fights_player_up": up_player,
                "disadvantage_fights_player_down": down_player
            }
        })

    return response_data


@app.get("/player/{player_id}/hero-first-death-rate")
def player_hero_first_death_rate(
        player_id: int,
        hero_id: Optional[int] = None,
        exclude_mirrors: Optional[bool] = False,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        map_played_id: Optional[int] = None,
        map_id: Optional[int] = None,
        match_id: Optional[int] = None,
        tournament_id: Optional[int] = None,
        team_id: Optional[int] = None,
        opponent_id: Optional[int] = None,
):
    base_query = """
        WITH filtered_first_deaths AS (
            SELECT 
                pts.hero_id,
                COUNT(*) AS total_fights,
                COUNT(*) FILTER (WHERE pts.player_id = tfe.first_death_player_id) AS first_deaths
            FROM player_timeseries pts 
            JOIN fact_team_fight_events tfe 
                ON pts.time_seconds = tfe.start_time 
                AND pts.map_played_id = tfe.map_played_id
            JOIN maps_played mp ON pts.map_played_id = mp.map_played_id
            JOIN matches m ON mp.match_id = m.match_id
            WHERE pts.player_id = %s {filters}
            GROUP BY pts.hero_id
        )
        SELECT 
            ffd.hero_id,
            COALESCE(ffd.total_fights, 0),
            COALESCE(ffd.first_deaths, 0)
        FROM filtered_first_deaths ffd
        WHERE 1 = 1 {hero_filter}
        ORDER BY ffd.hero_id ASC
    """

    response_filter = ""
    params = [player_id]

    if exclude_mirrors:
        response_filter += """ AND NOT (
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

    response_filter, params = add_start_date(start_date, response_filter, params)
    response_filter, params = add_end_date(end_date, response_filter, params)
    response_filter, params = add_map_played_id(map_played_id, response_filter, params)
    response_filter, params = add_map_id(map_id, response_filter, params)
    response_filter, params = add_match_id(match_id, response_filter, params)
    response_filter, params = add_tournament_id(tournament_id, response_filter, params)
    response_filter, params = add_team_id(team_id, response_filter, params, table_alias="pts")
    response_filter, params = add_opponent_id_by_compare(opponent_id, response_filter, params, table_alias="pts")

    hero_filter = ""
    if hero_id:
        hero_filter = " AND ffd.hero_id = %s"
        params.append(hero_id)

    formatted_query = base_query.format(filters=response_filter, hero_filter=hero_filter)
    cur.execute(formatted_query, params)
    results = cur.fetchall()

    response_data = []
    for row in results:
        h_id, total_fights, first_deaths = row
        first_death_rate = (first_deaths / total_fights * 100) if total_fights > 0 else 0.0

        response_data.append({
            "hero_id": h_id,
            "total_fights_participated": total_fights,
            "first_deaths_count": first_deaths,
            "first_death_rate": f"{first_death_rate:.2f}%"
        })

    return response_data

# PERKS