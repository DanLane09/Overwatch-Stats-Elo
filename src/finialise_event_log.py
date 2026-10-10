import pandas as pd
import re
import psycopg2
import numpy as np
from sklearn.cluster import DBSCAN
from pathlib import Path
from collections import defaultdict
import HeroDisplayNames
import requests
from config import API_KEY


"""conn = psycopg2.connect(host="localhost", port=5432, dbname="experiment_ow_stats_elo", user="postgres", password="pass")
cur = conn.cursor()"""

def format_hero_names_in_log(filename):
    with open(filename, "r", encoding="utf-8") as file:
        text = file.read()
    # Replace longest names first
    # so e.g. "soldier_76" is handled as one name.
    hero_pattern = re.compile(
        r"(?<![A-Za-z0-9_])("
        + "|".join(
            re.escape(hero)
            for hero in sorted(HeroDisplayNames.HERO_DISPLAY_NAMES, key=len, reverse=True)
        )
        + r")(?![A-Za-z0-9_])"
    )

    text = hero_pattern.sub(
        lambda match: HeroDisplayNames.HERO_DISPLAY_NAMES[match.group(1)],
        text
    )

    with open(filename, "w", encoding="utf-8") as file:
        file.write(text)

# Converts the hero names gained from the CSV and database into the same format
def transform_hero_name(hero_name: str) -> str | None:
    if pd.isna(hero_name) :
        return None
    for ch in ",.:":
        hero_name = hero_name.replace(ch, "")
    return hero_name.lower().replace(" ", "_")

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

def add_lines_chronologically(filename, new_lines, rounds):
    with open(filename, "r", encoding="utf-8") as file:
        lines = [line.rstrip("\n") for line in file]
    # Add new lines
    lines.extend(line.rstrip("\n") for line in new_lines)
    # Separate timestamped event lines from non-event lines
    event_lines = []
    non_event_lines = []

    final_time = None
    for line in lines:
        match = re.match(r"^\[(\d+)\]", line.strip())
        if match:
            event_lines.append(line)
            time = int(match.group(1))

            if final_time is None or time > final_time:
                final_time = time
        else:
            non_event_lines.append(line)


    log = f"[{final_time - 1}], Round {rounds} ended"
    event_lines.append(log)

    # Sort only timestamped events chronologically
    event_lines.sort(
        key=lambda line: int(
            re.match(r"^\[(\d+)\]", line.strip()).group(1)
        )
    )
    # Put non-event lines back at the end
    final_lines = event_lines + non_event_lines
    # Rewrite the file
    with open(filename, "w", encoding="utf-8") as file:
        for line in final_lines:
            file.write(line + "\n")

def detect_rounds(df: pd.DataFrame) -> list:
    counts = df.groupby('time').size()
    # Filter times with more than 50 snapshots
    round_starts = counts[counts > 60].index.tolist()
    return sorted(round_starts)

def detect_team_fights(df: pd.DataFrame):
    df = df.sort_values(["time", "original_index"])

    # Filter for 'Action Events'
    intensity_mask = (
            (df['deaths_delta'] > 0) |
            (df['ult_used'] == True) |
            (df['eliminations_delta'] > 0) |
            (df['assists_delta'] > 0) |
            (df['damage_delta'] > 75) |
            (df['healing_delta'] > 75) |
            (df['mitigated_delta'] > 75)
    )
    action_events = df[intensity_mask].copy()

    if action_events.empty:
        return 0, 0, 0

    # Reshape to (-1, 1) because DBSCAN expects a 2D array
    x = action_events['time'].values.reshape(-1, 1)

    # eps=4: Events within 4 seconds of each other belong to the same cluster
    # min_samples=15: A fight must have at least 15 events
    db = DBSCAN(eps=4, min_samples=15).fit(x)
    action_events['fight_label'] = db.labels_

    # Remove 'Noise' (label -1) and group by fight_label
    fights = action_events[action_events['fight_label'] != -1]

    if fights.empty:
        return 0, 0, 0

    start_times = []
    end_times = []
    winning_teams = []
    for fight_label, fight_df in fights.groupby("fight_label"):

        start_time = fight_df["time"].min()
        end_time = fight_df["time"].max()

        # TEAM KILLS
        team_kills = (
            fight_df.groupby("team_id")["eliminations_delta"]
            .sum()
            .to_dict()
        )

        # TEAM DAMAGE
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

        # WINNING TEAM
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
        start_times.append(start_time)
        end_times.append(end_time)
        winning_teams.append(winning_team_id)

    return start_times, end_times, winning_teams

def detect_ult_usage(df) -> pd.DataFrame:
    df = df.sort_values(['player_id', 'time'])

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

def _resolve_new_charge(t, current_hero_id, filtered_hero_id, min_charge_time,
                         start_charging_time, map_id, player_id, player_charge_events, row):
    """Evaluate and (if valid) log a charge-completion event. Returns charge_valid."""
    charge_time = t - start_charging_time

    if current_hero_id == filtered_hero_id:
        charge_valid = charge_time >= min_charge_time
    else:
        charge_valid = True

    if charge_valid:
        player_charge_events.append((
            map_id, player_id, current_hero_id,
            start_charging_time, t, charge_time, row['hero_name_clean']
        ))

    return charge_valid

def _died_between(group: pd.DataFrame, start_t, end_t) -> bool:
    """Whether this player has any death recorded within [start_t, end_t]."""
    return ((group['time'] >= start_t) & (group['time'] <= end_t) & (group['deaths_delta'] > 0)).any()


def count_fight_starts(df, start_times):
    snaps = df[["player_id", "hero_id", "time", "original_index"]].sort_values(
        ["player_id", "time", "original_index"]
    )
    counts = defaultdict(int)

    for start in start_times:
        # Latest snapshot per player at or before the fight start
        latest = snaps[snaps["time"] <= start].groupby("player_id").tail(1)
        for player_id, hero_id in zip(latest["player_id"], latest["hero_id"]):
            if pd.notna(hero_id):
                counts[(int(player_id), int(hero_id))] += 1

    return counts


def summarize_ults(charge_events, usage_events, fight_start_counts):
    stats = defaultdict(lambda: {"ults_used": 0, "total_charge_time": 0, "fight_starts": 0})

    for _, player_id, hero_id, _, _, charge_time, _ in charge_events:
        stats[(player_id, hero_id)]["total_charge_time"] += charge_time

    for _, player_id, hero_id, _, _, _, _ in usage_events:
        stats[(player_id, hero_id)]["ults_used"] += 1

    for (player_id, hero_id), n in fight_start_counts.items():
        stats[(player_id, hero_id)]["fight_starts"] += n

    return [
        {
            "player_id": player_id,
            "hero_id": hero_id,
            "ults_used": s["ults_used"],
            "total_charge_time": s["total_charge_time"],
            "fight_starts": s["fight_starts"],
        }
        for (player_id, hero_id), s in stats.items()
    ]

def build_fight_payloads(match_map, df, start_times, end_times, winning_teams,
                         blue_team_id, red_team_id, ult_used, event_log_file):
    """Build one payload per fight. team1 = blue/left team, team2 = red/right team."""
    player_team = df.drop_duplicates("player_id").set_index("player_id")["team_id"].to_dict()

    with open(event_log_file, "r", encoding="utf-8") as file:
        lines = [line.rstrip("\n") for line in file]
    # Parse "[time], text" log lines into dicts once
    parsed_events = []
    for line in lines:
        m = re.match(r"^\[(\d+)\],\s*(.*)$", line.strip())
        if m:
            parsed_events.append({"time": int(m.group(1)), "event": m.group(2)})

    payloads = []
    for i, (start, end, winner) in enumerate(zip(start_times, end_times, winning_teams)):
        start, end = int(start), int(end)
        window = df[(df["time"] >= start) & (df["time"] <= end)]

        def team_sum(team_id, col):
            return int(window.loc[window["team_id"] == team_id, col].sum())

        loser = red_team_id if winner == blue_team_id else blue_team_id

        team1_ults, team2_ults = [], []
        for _, player_id, hero_id, _, used_time, _, _ in ult_used:
            if start <= used_time <= end and hero_id is not None:
                entry = {"hero": int(hero_id)}
                if player_team.get(player_id) == blue_team_id:
                    team1_ults.append(entry)
                else:
                    team2_ults.append(entry)

        payloads.append({
            "matchMap": match_map,
            "fightNumber": i + 1,
            "fightStartTime": start,
            "fightLength": end - start,
            "winningTeam": int(winner),
            "losingTeam": int(loser),
            "team1Kills": team_sum(red_team_id, "deaths_delta"),
            "team2Kills": team_sum(blue_team_id, "deaths_delta"),
            "team1Deaths": team_sum(blue_team_id, "deaths_delta"),
            "team2Deaths": team_sum(red_team_id, "deaths_delta"),
            "team1Ults": team1_ults,
            "team2Ults": team2_ults,
            "eventLog": {"events": [e for e in parsed_events if start <= e["time"] <= end]},
        })
    return payloads, parsed_events


def post_fights(match_map, api_url, df, start_times, end_times, winning_teams,
                blue_team_id, red_team_id, ult_used, event_log_file,
                headers=None, timeout=10):
    payloads, event_log = build_fight_payloads(match_map, df, start_times, end_times, winning_teams,
                                    blue_team_id, red_team_id, ult_used, event_log_file)
    results = []
    for payload in payloads:
        try:
            response = requests.post(api_url, json=payload, headers = {"X-API-Key": API_KEY}, timeout=timeout)
            response.raise_for_status()
            results.append((payload["fightNumber"], response.status_code, None))
        except requests.RequestException as e:
            results.append((payload["fightNumber"], None, str(e)))
            print(f"Fight {payload['fightNumber']} failed: {e}")
    return results, event_log

def process_and_save_ults(df: pd.DataFrame, map_id: int, hero_map: dict, round_starts: list):
    df = df.sort_values(['player_id', 'time', 'original_index']).copy()
    df['status_change'] = df.groupby('player_id')['ult_charged'].shift() != df['ult_charged']
    df['hero_name_clean'] = df['hero'].str.lower().str.replace(r'[.,:]', '', regex=True).str.replace(' ', '_')
    df['hero_change'] = df.groupby('player_id')['hero_name_clean'].shift() != df['hero_name_clean']
    df['is_round_start'] = df['time'].isin(round_starts)

    charge_events = []
    usage_events = []
    special_recharge_heroes = {8, 53} # D.Va, D.Mon
    # D.Va ult charge reverts to ult charge in mech upon death, but D.Mon reverts upon respawn so we have to account for wave respawns (6s-12s)
    if special_recharge_heroes is None:
        special_recharge_heroes = set()
    else:
        special_recharge_heroes = set(special_recharge_heroes)

    for player_id, group in df.groupby('player_id'):
        # Initial trackers
        start_charging_time = group['time'].iloc[0]
        start_holding_time = None
        current_hero_id = hero_map.get(group['hero_name_clean'].iloc[0])
        charge_valid = False
        player_charge_events = []
        player_usage_events = []

        # Deferred-resolution tracker for mech-style heroes (D.Va, D.Mon, ...)
        pending_loss_time = None  # time of an unresolved True->False flip, or None

        # Filter for rows where state resets or flips
        triggers = group[(group['status_change']) | (group['hero_change']) | (group['is_round_start'])]

        for _, row in triggers.iterrows():
            t = row['time']

            # --- RESET LOGIC (Hero Swap or Round Start) ---
            if row['hero_change'] or row['is_round_start']:
                # Resolve any dangling pending loss before resetting
                if pending_loss_time is not None:
                    if not _died_between(group, pending_loss_time, t) and charge_valid:
                        player_usage_events.append((
                            map_id, player_id, current_hero_id,
                            start_holding_time, pending_loss_time, pending_loss_time - start_holding_time, row['hero_name_clean']
                        ))
                    # If a death occurred, the ult was force-lost with no real use — nothing to log

                start_charging_time = t
                start_holding_time = None
                current_hero_id = hero_map.get(row['hero_name_clean'])
                charge_valid = False
                pending_loss_time = None
                continue

            # --- COMPLETION LOGIC ---
            # Ult Gained (End of Charge / Start of Hold, or a mech regaining charge)
            if row['ult_charged'] == True:

                if current_hero_id in special_recharge_heroes and pending_loss_time is not None:
                    if _died_between(group, pending_loss_time, t):
                        # Forced mech loss, not a real use — discard, keep the original hold running
                        pending_loss_time = None
                        continue
                    else:
                        # No death in between: the earlier drop really was a usage
                        if charge_valid:
                            player_usage_events.append((
                                map_id, player_id, current_hero_id,
                                start_holding_time, pending_loss_time, pending_loss_time - start_holding_time, row['hero_name_clean']
                            ))
                        start_charging_time = pending_loss_time
                        pending_loss_time = None
                        # falls through to normal new-charge logic below, using start_charging_time set above

                charge_valid = _resolve_new_charge(
                    t, current_hero_id, 11, 16,
                    start_charging_time, map_id, player_id, player_charge_events, row
                )
                start_holding_time = t

            # Ult Used (End of Hold / Start of New Charge) — or possibly a mech loss
            elif row['ult_charged'] == False and start_holding_time is not None:
                if current_hero_id in special_recharge_heroes:
                    # Don't decide yet — wait to see if it recharges with a death in between
                    pending_loss_time = t
                else:
                    if charge_valid:
                        player_usage_events.append((
                            map_id, player_id, current_hero_id,
                            start_holding_time, t, t - start_holding_time, row['hero_name_clean']
                        ))
                    start_charging_time = t
                    start_holding_time = None
                    charge_valid = False

        for i in range(len(player_charge_events)):
            charge_events.append(player_charge_events[i])
        for i in range(len(player_usage_events)):
            usage_events.append(player_usage_events[i])

    return charge_events, usage_events

def process_and_save_perks(df: pd.DataFrame, map_id: int, hero_map: dict, round_starts: list):
    df = df.sort_values(["player_id", "time", "original_index"]).copy()

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

    # One list containing BOTH minor and major perk events
    perk_events = []

    for (player_id, round_id), group in df.groupby(
            ["player_id", "round_id"]
    ):
        group = group.sort_values(["time", "original_index"])

        # Total time spent on each hero during this round
        hero_time_acc = {}

        # Track perks already unlocked for this player/round
        # (hero_id, perk_type)
        unlocked_perks = set()

        last_time = None
        last_hero = None

        for _, row in group.iterrows():

            t = row["time"]
            hero_id = row["hero_id"]

            if pd.isna(hero_id):
                continue

            hero_id = int(hero_id)

            # TIME ACCUMULATION
            if last_time is not None and last_hero is not None:
                dt = t - last_time

                hero_time_acc[last_hero] = (
                        hero_time_acc.get(last_hero, 0) + dt
                )

            # PERK DETECTION
            for perk_type in ("minor", "major"):
                perk_name = row[f"{perk_type}_perk"]
                # No perk selected
                if pd.isna(perk_name):
                    continue

                # Identify this specific perk type for this hero
                perk_key = (hero_id, perk_type)

                # Already recorded this hero's perk this round
                if perk_key in unlocked_perks:
                    continue

                # Total time spent playing this hero this round
                total_time = hero_time_acc.get(hero_id, 0)
                # Add event
                perk_events.append((
                    int(player_id),
                    hero_id,
                    perk_type,
                    str(perk_name),
                    int(t),
                    int(total_time),
                    row["hero_name_clean"]
                ))
                # Mark as recorded
                unlocked_perks.add(perk_key)

            # UPDATE HERO TRACKING
            last_time = t
            last_hero = hero_id

    return perk_events

def main(csv_path, event_log_path, left_team_name, right_team_name, left_team_id, right_team_id, map_type, players, heroes, match_map_id):
    add_to_log = []
    csv_path = Path(csv_path)
    with open(csv_path, "r") as f:
        leftover, map_played_id = csv_path.name.split("map_played_id-")
        map_played_id = int(map_played_id.rstrip(".csv"))
        blue_team_id, red_team_id, map_type = left_team_id, right_team_id, map_type

        df = pd.read_csv(f)
        df["original_index"] = range(len(df))
        df["team_id"] = np.where((df["original_index"] % 10) < 5, blue_team_id, red_team_id)  # Indexes 0-4 are blue team, indexes 5-9 are red team
        df["map_played_id"] = map_played_id

        df["hero_id"] = df["hero"].apply(transform_hero_name).map(heroes)
        df["hero_id"] = df["hero_id"].astype("Int64")

        df = chunk_time(df=df)
        df = calculate_stat_deltas(df=df)
        df = detect_ult_usage(df=df)

        start_times, end_times, winning_teams = detect_team_fights(df=df)
        fight_start_counts = count_fight_starts(df, start_times)
        for i in range(len(start_times)):
            log = f"[{start_times[i]}], Fight {i + 1} started"
            add_to_log.append(log)
            if winning_teams[i] == blue_team_id:
                log = f"[{end_times[i]}], Fight {i + 1} ended with {left_team_name} winning"
            elif winning_teams[i] == red_team_id:
                log = f"[{end_times[i]}], Fight {i + 1} ended with {right_team_name} winning"
            else:
                log = f"[{end_times[i]}], Fight {i + 1} ended with neither team winning"
            add_to_log.append(log)

        rounds = detect_rounds(df=df)
        num_rounds = len(rounds)
        for i in range(len(rounds)):
            log = f"[{rounds[i]}], Round {i + 1} started"
            add_to_log.append(log)
            if rounds[i] > 0:
                log = f"[{rounds[i] - 1}], Round {i} ended"
                add_to_log.append(log)

        ult_charged, ult_used = process_and_save_ults(df=df, map_id=map_played_id, hero_map=heroes, round_starts=rounds)
        for i in range(len(ult_charged)):
            log = f"[{ult_charged[i][4]}], {players[ult_charged[i][1]]} has charged {ult_charged[i][6]}'s ultimate, Charge time: {ult_charged[i][5]}s"
            add_to_log.append(log)
        for i in range(len(ult_used)):
            log = f"[{ult_used[i][4]}], {players[ult_used[i][1]]} has used {ult_used[i][6]}'s ultimate, Hold time: {ult_used[i][5]}s"
            add_to_log.append(log)

        ult_summary = summarize_ults(charge_events=ult_charged, usage_events=ult_used, fight_start_counts=fight_start_counts)

        if map_type not in ("escort", "hybrid"):
            rounds = []
        perks = process_and_save_perks(df=df, map_id=map_played_id, hero_map=heroes, round_starts=rounds)
        for i in range(len(perks)):
            log = f"[{perks[i][4]}], {players[perks[i][0]]} has selected {perks[i][2]} perk [{perks[i][3]}] for {perks[i][6]}"
            add_to_log.append(log)

    add_lines_chronologically(event_log_path, add_to_log, num_rounds)
    format_hero_names_in_log(event_log_path)

    results, event_log = post_fights(match_map=match_map_id, api_url="https://owtv.gg/api/map-fight", df=df, start_times=start_times,
                end_times=end_times, winning_teams=winning_teams,
                blue_team_id=blue_team_id, red_team_id=red_team_id, ult_used=ult_used, event_log_file=event_log_path)

    return ult_summary, event_log

"""shutil.move(file.path, "./Game CSVs/Processed/")
print(f"Ingested {f.name}")"""

"""x, y = main("../Game CSVs/99DIVINE vs MURASH GAMING --- match_id-1628, map_played_id-3852.csv",
     "../Game Logs/99DIVINE vs MURASH GAMING --- match_id-1628, map_played_id-3852.txt",
     "99DIVINE", "MURASH GAMING", 149, 228, "flashpoint",
     players={1521:"MN3", 1280: "ALTHOUGH", 488: "Ichi", 163: "Sakume", 877: "Umi", 159: "ky0n", 236: "Viper", 447: "PEPPI", 332: "epic", 477: "orca"},
     heroes={'doctrine': 54, 'dmon': 53, 'shion': 52, 'zenyatta': 51, 'zarya': 50, 'wuyang': 49, 'wrecking_ball': 48, 'winston': 47, 'widowmaker': 46, 'venture': 45, 'vendetta': 44, 'tracer': 43, 'torbjorn': 42, 'symmetra': 41, 'sombra': 40, 'soldier_76': 39, 'sojourn': 38, 'sigma': 37, 'sierra': 36, 'roadhog': 35, 'reinhardt': 34, 'reaper': 33, 'ramattra': 32, 'pharah': 31, 'orisa': 30, 'moira': 29, 'mizuki': 28, 'mercy': 27, 'mei': 26, 'mauga': 25, 'lucio': 24, 'lifeweaver': 23, 'kiriko': 22, 'juno': 21, 'junkrat': 20, 'junker_queen': 19, 'jetpack_cat': 18, 'illari': 17, 'hazard': 16, 'hanzo': 15, 'genji': 14, 'freja': 13, 'emre': 12, 'echo': 11, 'doomfist': 10, 'domina': 9, 'dva': 8, 'cassidy': 7, 'brigitte': 6, 'bastion': 5, 'baptiste': 4, 'ashe': 3, 'anran': 2, 'ana': 1},
     match_map_id=3852)"""