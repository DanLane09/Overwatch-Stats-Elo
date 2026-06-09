import shutil
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import io
from sklearn.cluster import DBSCAN

conn = psycopg2.connect(host="localhost", port=5432, dbname="experiment_ow_stats_elo", user="postgres", password="pass")
cur = conn.cursor()

# Converts the hero names gained from the CSV and database into the same format
def transform_hero_name(hero_name: str) -> str | None:
    if pd.isna(hero_name) :
        return None
    for ch in ",.:":
        hero_name = hero_name.replace(ch, "")
    return hero_name.lower().replace(" ", "_")

# Fixes issues where perks aren't seen for random timesteps
def solve_perk_abnormalities(df: pd.DataFrame) -> pd.DataFrame:
    columns = ["minor_perk", "major_perk"]
    df = df.sort_values(["player_id", "original_index"]).copy()
    for col in columns:
        is_none = df[col].isna()
        # Group by contiguous blocks of NaN/not-NaN for the same player
        group_id = (is_none.ne(is_none.shift()) | (df["player_id"] != df["player_id"].shift())).cumsum()
        run_lengths = is_none.groupby(group_id).transform("sum")
        short_none_mask = is_none & (run_lengths <= 2)
        filled = df.groupby("player_id")[col].ffill()
        df.loc[short_none_mask, col] = filled[short_none_mask]
    return df

# Converts multiple entries from the same second into continuing time-series data. Maximum if 10 entries at the same time-step to mirror the 10 players.
def chunk_time(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["time", "original_index"])

    group_sizes = df.groupby("time")["time"].transform("size")
    chunks = np.ceil(group_sizes / 10)
    step = 1 / chunks

    pos = df.groupby("time").cumcount()
    df["time_seconds"] = df["time"] + (pos // 10) * step
    return df

def calculate_stat_deltas(df: pd.DataFrame) -> pd.DataFrame:
    values = ["eliminations", "assists", "deaths", "damage", "healing", "mitigated"]
    for col in values:
        df[f"{col}_delta"] = (
            df.groupby("player_id")[col].diff()
        ).fillna(0).clip(lower=0).astype(int)
    return df

def detect_ult_usage(df) -> pd.DataFrame:
    df = df.sort_values(['player_id', 'time_seconds'])

    # Look for the 'State Flip' (True -> False)
    # .shift(1) looks at the previous row
    prev_ult = df.groupby('player_id')['ult_charged'].shift(1)
    prev_hero = df.groupby('player_id')['hero'].shift(1)

    # Condition: Prev was True, Current is False, Hero is the same
    df['ult_used'] = (
            (prev_ult == True) &
            (df['ult_charged'] == False) &
            (df['hero'] == prev_hero)
    )
    return df


def detect_rounds(df: pd.DataFrame, map_played_id: int) -> list:
    """
    Identifies round starts based on high snapshot density (>50 entries at one timestamp).
    Saves them to the database and returns a list of round start times.
    """
    counts = df.groupby('time').size()
    # Filter times with more than 50 snapshots
    round_starts = counts[counts > 50].index.tolist()

    # Save to database
    for i, start_time in enumerate(round_starts):
        cur.execute("""
            INSERT INTO fact_round_start_events (map_played_id, round_number, start_time)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (map_played_id, i + 1, start_time))

    return sorted(round_starts)


def detect_team_fights(df: pd.DataFrame, map_played_id: int) -> None:
    """
    Uses DBSCAN to cluster deaths and ultimate usages into cohesive team fights.
    """
    # Filter for 'Action Events'
    intensity_mask = (
            (df['deaths_delta'] > 0) |
            (df['ult_used'] == True) |
            (df['eliminations_delta'] > 0) |
            (df['assists_delta'] > 0) |
            (df['damage_delta'] > 75) |  # Trigger if > 150 damage in a single snapshot
            (df['healing_delta'] > 75) |  # Trigger if > 100 healing in a single snapshot
            (df['mitigated_delta'] > 75)  # Trigger if > 150 mitigated in a single snapshot
    )
    action_events = df[intensity_mask].copy()

    if action_events.empty:
        return

    # Prepare data for DBSCAN (clustering based on time_seconds)
    # We reshape to (-1, 1) because DBSCAN expects a 2D array
    X = action_events['time_seconds'].values.reshape(-1, 1)

    # eps=4: Events within 4 seconds of each other belong to the same cluster
    # min_samples=15: A fight must have at least 15 events
    db = DBSCAN(eps=4, min_samples=15).fit(X)
    action_events['fight_label'] = db.labels_

    # Remove 'Noise' (label -1) and group by fight_label
    fights = action_events[action_events['fight_label'] != -1]

    if fights.empty:
        return

    fight_records = []
    for fight_label, fight_df in fights.groupby("fight_label"):

        start_time = fight_df["time_seconds"].min()
        end_time = fight_df["time_seconds"].max()

        # ----------------------------------------
        # TEAM KILLS
        # ----------------------------------------

        team_kills = (
            fight_df.groupby("team_id")["eliminations_delta"]
            .sum()
            .to_dict()
        )

        # ----------------------------------------
        # TEAM DAMAGE
        # ----------------------------------------

        team_damage = (
            fight_df.groupby("team_id")["damage_delta"]
            .sum()
            .to_dict()
        )

        team_ids = list(team_kills.keys())

        if len(team_ids) < 2:
            continue

        team_a = team_ids[0]
        team_b = team_ids[1]

        kills_a = team_kills.get(team_a, 0)
        kills_b = team_kills.get(team_b, 0)

        # ----------------------------------------
        # WINNING TEAM
        # ----------------------------------------

        if kills_a > kills_b:
            winning_team_id = team_a

        elif kills_b > kills_a:
            winning_team_id = team_b

        else:
            damage_a = team_damage.get(team_a, 0)
            damage_b = team_damage.get(team_b, 0)

            if damage_a >= damage_b:
                winning_team_id = team_a
            else:
                winning_team_id = team_b

        # ----------------------------------------
        # FIRST DEATH
        # ----------------------------------------

        death_rows = (
            fight_df[fight_df["deaths_delta"] > 0]
            .sort_values("time_seconds")
        )

        first_death_player_id = None

        if not death_rows.empty:
            first_death_player_id = int(
                death_rows.iloc[0]["player_id"]
            )

        fight_records.append((
            map_played_id,
            start_time,
            end_time,
            int(winning_team_id),
            first_death_player_id
        ))


    fight_summary = fights.groupby('fight_label').agg(
        start_time=('time_seconds', 'min'),
        end_time=('time_seconds', 'max'),
        total_kills=('deaths_delta', 'sum'),
        total_ults=('ult_used', 'sum'),
        total_damage=('damage_delta', 'sum'),
        total_healing=('healing_delta', 'sum'),
        total_mitigation=('mitigated_delta', 'sum'),
        total_elims=('eliminations_delta', 'sum')
    ).reset_index()



    if fight_records:
        query = """
                INSERT INTO fact_team_fight_events 
                (map_played_id, start_time, end_time, winning_team_id, first_death_player_id)
                VALUES %s
            """
        extras.execute_values(cur, query, fight_records)


def process_and_save_ults(df: pd.DataFrame, map_id: int, hero_map: dict, round_starts: list) -> None:
    """
    Calculates completed ult cycles and saves them to the database.
    Discarded: cycles interrupted by hero swaps or round starts.
    """
    # 1. Prepare data for analysis
    df = df.sort_values(['player_id', 'time_seconds']).copy()
    df['status_change'] = df.groupby('player_id')['ult_charged'].shift() != df['ult_charged']
    df['hero_name_clean'] = df['hero'].str.lower().str.replace(r'[.,:]', '', regex=True).str.replace(' ', '_')
    df['hero_change'] = df.groupby('player_id')['hero_name_clean'].shift() != df['hero_name_clean']
    df['is_round_start'] = df['time'].isin(round_starts)

    charge_events = []
    usage_events = []

    for player_id, group in df.groupby('player_id'):
        # Initial trackers
        start_charging_time = group['time_seconds'].iloc[0]
        start_holding_time = None
        current_hero_id = hero_map.get(group['hero_name_clean'].iloc[0])

        # Filter for rows where state resets or flips
        triggers = group[(group['status_change']) | (group['hero_change']) | (group['is_round_start'])]

        for _, row in triggers.iterrows():
            t = row['time_seconds']

            # --- RESET LOGIC (Hero Swap or Round Start) ---
            if row['hero_change'] or row['is_round_start']:
                start_charging_time = t
                start_holding_time = None
                current_hero_id = hero_map.get(row['hero_name_clean'])
                continue

            # --- COMPLETION LOGIC ---
            # Ult Gained (End of Charge / Start of Hold)
            if row['ult_charged'] == True:
                charge_events.append((
                    map_id, player_id, current_hero_id,
                    start_charging_time, t, t - start_charging_time
                ))
                start_holding_time = t

            # Ult Used (End of Hold / Start of New Charge)
            elif row['ult_charged'] == False and start_holding_time is not None:
                usage_events.append((
                    map_id, player_id, current_hero_id,
                    start_holding_time, t, t - start_holding_time
                ))
                start_charging_time = t
                start_holding_time = None

    # 2. Bulk Insert to Database
    if charge_events:
        charge_query = """
            INSERT INTO fact_ult_charge_events 
            (map_played_id, player_id, hero_id, charge_start_time, charge_end_time, charge_duration)
            VALUES %s
        """
        extras.execute_values(cur, charge_query, charge_events)

    if usage_events:
        usage_query = """
            INSERT INTO fact_ult_usage_events 
            (map_played_id, player_id, hero_id, ult_ready_time, ult_used_time, hold_duration)
            VALUES %s
        """
        extras.execute_values(cur, usage_query, usage_events)


def process_and_save_hero_swaps(df: pd.DataFrame,map_id: int) -> None:
    df = df.sort_values(["player_id", "time_seconds"]).copy()

    df["prev_hero_id"] = df.groupby("player_id")["hero_id"].shift(1)

    swaps = df[
        ((df["hero_id"].notna()) &
        (df["hero_id"] != df["prev_hero_id"])) |
        ((df["time_seconds"] == 0) &
        (df["hero_id"].notna())) |
        ((df["prev_hero_id"].isna()) &
         (df["hero_id"].notna()))
        ]

    swap_events = [
        (
            int(map_id),
            int(row["player_id"]),
            None if pd.isna(row["prev_hero_id"]) else int(row["prev_hero_id"]),
            int(row["hero_id"]),
            float(row["time_seconds"]),
        )
        for _, row in swaps.iterrows()
    ]

    # --- BULK INSERT ---
    if swap_events:
        query = """
            INSERT INTO fact_hero_swap_events
            (map_played_id, player_id, old_hero_id, new_hero_id, event_time)
            VALUES %s
        """
        extras.execute_values(cur, query, swap_events)


def process_and_save_perks(df: pd.DataFrame, map_id: int, hero_map: dict, round_starts: list) -> None:
    df = df.sort_values(["player_id", "time_seconds"]).copy()

    # Clean hero names → hero_id
    df["hero_name_clean"] = df["hero"].apply(transform_hero_name)
    df["hero_id"] = df["hero_name_clean"].map(hero_map)

    # Round assignment
    round_starts_sorted = sorted(round_starts)

    def assign_round(t):
        r = 0
        for i, start in enumerate(round_starts_sorted):
            if t >= start:
                r = i
            else:
                break
        return r

    df["round_id"] = df["time"].apply(assign_round)

    perk_events = []

    for (player_id, round_id), group in df.groupby(["player_id", "round_id"]):
        group = group.sort_values("time_seconds")

        hero_time_acc = {}
        last_time = None
        last_hero = None

        first_row = group.iloc[0]
        prev_minor = first_row["minor_perk"]
        prev_major = first_row["major_perk"]

        for _, row in group.iterrows():
            t = row["time_seconds"]
            hero_id = row["hero_id"]

            if pd.isna(hero_id):
                continue

            # --- TIME ACCUMULATION ---
            if last_time is not None and last_hero is not None:
                dt = t - last_time
                hero_time_acc[last_hero] = hero_time_acc.get(last_hero, 0) + dt

            # --- PERK DETECTION ---
            # Minor perk unlock
            if pd.notna(row["minor_perk"]) and pd.isna(prev_minor):
                total_time = hero_time_acc.get(hero_id, 0)
                perk_events.append((
                    int(map_id),
                    int(player_id),
                    int(hero_id),
                    "minor",
                    str(row["minor_perk"]),
                    float(t),
                    float(total_time)
                ))

            # Major perk unlock
            if pd.notna(row["major_perk"]) and pd.isna(prev_major):
                total_time = hero_time_acc.get(hero_id, 0)
                perk_events.append((
                    int(map_id),
                    int(player_id),
                    int(hero_id),
                    "major",
                    str(row["major_perk"]),
                    float(t),
                    float(total_time)
                ))

            # Update trackers
            prev_minor = row["minor_perk"]
            prev_major = row["major_perk"]
            last_time = t
            last_hero = hero_id

    # --- BULK INSERT ---
    if perk_events:
        query = """
            INSERT INTO fact_perk_events
            (map_played_id, player_id, hero_id, perk_type, perk_name, unlock_time, total_charge_time)
            VALUES %s
        """
        extras.execute_values(cur, query, perk_events)

def ingest_timeseries(df: pd.DataFrame, table_name: str) -> None:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    column_names = df.columns.tolist()
    query = f"COPY {table_name} ({', '.join(column_names)}) FROM STDIN WITH CSV"
    cur.copy_expert(query, buffer)

def ingest_to_db(path) -> None:
    for file in os.scandir(path):
        if file.is_file() and file.name.endswith(".csv"):
            with open(file, "r") as f:
                leftover, map_played_id = file.name.split("map_played_id-")
                map_played_id = int(map_played_id.rstrip(".csv"))

                cur.execute("""SELECT mp.blue_team_id, mp.red_team_id, m.map_type_id FROM maps_played mp JOIN maps m ON mp.map_id = m.map_id WHERE mp.map_played_id = %s""", (map_played_id,))
                blue_team_id, red_team_id, map_type_id = cur.fetchone()
                map_type_id = int(map_type_id)

                cur.execute("""SELECT * FROM heroes""")
                reverse_heroes = dict(cur.fetchall())
                heroes = {transform_hero_name(v): k for k, v in reverse_heroes.items()}


                df = pd.read_csv(f)
                df["original_index"] = range(len(df))
                df["team_id"] = np.where((df["original_index"] % 10) < 5, blue_team_id, red_team_id)  # Indexes 0-4 are blue team, indexes 5-9 are red team
                df["map_played_id"] = map_played_id

                df["hero_id"] = df["hero"].apply(transform_hero_name).map(heroes)
                df["hero_id"] = df["hero_id"].astype("Int64")

                df = solve_perk_abnormalities(df=df)
                df = calculate_stat_deltas(df=df)
                df = chunk_time(df=df)
                df = detect_ult_usage(df=df)

                rounds = detect_rounds(df=df, map_played_id=map_played_id)
                process_and_save_ults(df=df, map_id=map_played_id, hero_map=heroes, round_starts=rounds)
                process_and_save_hero_swaps(df=df,map_id=map_played_id)
                if map_type_id not in [3, 5]:
                    rounds = []
                process_and_save_perks(df=df, map_id=map_played_id, hero_map=heroes, round_starts=rounds)
                detect_team_fights(df=df, map_played_id=map_played_id)

                # Creating DataFrame to pass to database
                cols = ["map_played_id", "player_id", "time_seconds", "hero_id", "minor_perk", "major_perk", "ult_used", "eliminations_delta", "assists_delta", "deaths_delta", "damage_delta", "healing_delta", "mitigated_delta", "team_id"]
                final_df = df[cols]
                ingest_timeseries(df=final_df, table_name="player_timeseries")
                conn.commit()

            # Move processed map to different folder
            shutil.move(file.path, "./Game CSVs/Processed/")
            print(f"Ingested {f.name}")


if __name__ == "__main__":
    csv_path = "Game CSVs"
    ingest_to_db(csv_path)