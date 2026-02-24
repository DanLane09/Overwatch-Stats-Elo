import psycopg2.extras
from collections import Counter
import sys


conn = psycopg2.connect(host="localhost", port=5432, dbname="ow_stats_elo", user="postgres", password="pass")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


two_map_actual = {"1": 0.75, "0": 1}
three_map_actual = {"2": 0.55, "1": 0.75, "0": 1}
four_map_actual = {"3": 0.55, "2": 0.65, "1": 0.80, "0": 1}

PLAYER_SCORE_DECAY = 0.90
TEAM_ELO_K = 25

def calculate_fantasy_score(kills, deaths, assists, damage, healing, mitigated):
    return (kills // 3) + (((damage + healing) // 1000) * 0.5) - deaths

def calculate_player_cps(kills, deaths, assists, damage, healing, mitigated, fantasy_score):
    w1 = 1.0; w2 = 0.6; w3 = 3; w4 = 0.001; w5 = 0.001; w6 = 0.0012
    ris = (kills * w1 + assists * w2 - deaths * w3 + damage * w4 + healing * w5 + mitigated * w6)
    fm = 1 + (fantasy_score / 100.0)
    return ris * fm

def calculate_role_cps(kills, deaths, assists, damage, healing, mitigated, role):
    if role == "Tank":
        w = {"kills": 0.8, "assists": 0.5, "deaths": 2.5, "damage": 0.0008, "healing": 0.0001, "mitigated": 0.0020}
    elif role == "DPS":
        w = {"kills": 1.4, "assists": 0.4, "deaths": 2, "damage": 0.0014, "healing": 0.0002, "mitigated": 0.0001}
    elif role == "Support":
        w = {"kills": 0.6, "assists": 0.9, "deaths": 2.25, "damage": 0.0004, "healing": 0.0022, "mitigated": 0.0002}
    else:
        w = {"kills": 1.0, "assists": 0.6, "deaths": 2.25, "damage": 0.001, "healing": 0.001, "mitigated": 0.001}
    score = (kills * w["kills"] + assists * w["assists"] - deaths * w["deaths"] + damage * w["damage"] + healing * w["healing"] + mitigated * w["mitigated"])
    return score

def get_normalized_match_stats(match_id, player_id):
    cur.execute("""
        SELECT COUNT(*) FROM player_map_stats
        WHERE player_id = %s AND map_played_id IN (SELECT match_map_id FROM match_maps WHERE match_id = %s);
    """, (player_id, match_id))
    num_maps = cur.fetchone()[0] or 1
    cur.execute("""
        SELECT eliminations, deaths, assists, damage, healing, mitigated, fantasy_score
        FROM player_match_stats
        WHERE match_id = %s AND player_id = %s;
    """, (match_id, player_id))
    row = cur.fetchone()
    if not row:
        return None
    kills, deaths, assists, damage, healing, mitigated, fantasy_score = row
    norm = {
        "eliminations": kills / num_maps,
        "deaths": deaths / num_maps,
        "assists": assists / num_maps,
        "damage": damage / num_maps,
        "healing": healing / num_maps,
        "mitigated": mitigated / num_maps,
        "fantasy": fantasy_score / num_maps,
        "maps": num_maps
    }
    return norm

def update_team_elo(winning_team_old_elo, losing_team_old_elo, actual_score, winning_team_id, losing_team_id, match_id):
    k = TEAM_ELO_K
    winning_team_expected_score = 1 / (1 + 10 ** ((losing_team_old_elo - winning_team_old_elo) / 400))
    winning_team_new_elo = winning_team_old_elo + k * (actual_score - winning_team_expected_score)
    losing_team_expected_score = 1 / (1 + 10 ** ((winning_team_old_elo - losing_team_old_elo) / 400))
    losing_team_new_elo = losing_team_old_elo + k * ((1 - actual_score) - losing_team_expected_score)

    cur.execute("UPDATE teams SET elo = %s WHERE team_id = %s;", (winning_team_new_elo, winning_team_id))
    cur.execute("INSERT INTO elo_history (entity_type, entity_id, old_elo, new_elo, match_id) VALUES (%s, %s, %s, %s, %s)",
                ("team", winning_team_id, winning_team_old_elo, winning_team_new_elo, match_id))

    cur.execute("UPDATE teams SET elo = %s WHERE team_id = %s;", (losing_team_new_elo, losing_team_id))
    cur.execute("INSERT INTO elo_history (entity_type, entity_id, old_elo, new_elo, match_id) VALUES (%s, %s, %s, %s, %s)",
                ("team", losing_team_id, losing_team_old_elo, losing_team_new_elo, match_id))
    conn.commit()

# =========================
# Your existing functions for adding matches, teams, players, maps, players stats, transfers, complete_match
# (I include them verbatim but with a small change: complete_match will call propagate_result at the end)
# =========================

def create_match():
    cur.execute("""
        SELECT name, tournament_id FROM tournaments;
    """)
    tournaments = cur.fetchall()
    for i in range(len(tournaments)):
        print(f"{i + 1}) {tournaments[i][0]}")
    number = int(input("Select tournament (use number from above): "))

    cur.execute("""
        SELECT t.team_id, t.name FROM teams t
        JOIN tournament_teams tt ON t.team_id = tt.team_id
        WHERE tt.tournament_id = %s
        ORDER BY tt.seed ASC;
    """, (tournaments[number - 1][1], ))
    teams = cur.fetchall()
    for i in range(len(teams)):
        print(f"{i + 1}) {teams[i][1]}")
    team_a_id = teams[int(input("Select the upper seeded team (number from above): ")) - 1][0]
    team_b_id = teams[int(input("Select the lower seeded team (number from above): ")) - 1][0]

    date_str = input("Enter date and time (YYYY-MM-DD HH:MM:SS): ")
    offset = input("Enter timezone offset (e.g., +3, -4, +5.5): ").strip()
    match_type = str(input("Enter the match type: "))
    first_to = int(input("First to (e.g. 2, 3, 4): "))
    # version = input("Enter game version: ")
    version = "2.21.0.0.146669"

    if "." in offset:
        hours, minutes = offset.split(".")
        minutes = int(float("0." + minutes) * 60)
        offset = f"{int(hours):+03d}:{minutes:02d}"
    else:
        offset = f"{int(offset):+03d}"
    timestamp_with_offset = f"{date_str}{offset}"

    cur.execute("""
        INSERT INTO matches (date_played, tournament_id, match_type, game_version, first_to, team_a_id, team_b_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING match_id;
    """, (timestamp_with_offset, tournaments[number - 1][1], match_type, version, first_to, team_a_id, team_b_id))
    mid = cur.fetchone()[0]
    conn.commit()
    print(f"Created match {mid}")

def create_team():
    name = str(input("Enter team name: "))
    region = str(input("Enter team region: "))
    cur.execute("INSERT INTO teams (name, region) VALUES (%s, %s) RETURNING team_id;", (name, region))
    tid = cur.fetchone()[0]
    conn.commit()
    print(f"Created team {tid} - {name}")

def create_player():
    name = str(input("Enter player name: "))
    role = str(input("Enter role: "))
    cur.execute("INSERT INTO players (name, role) VALUES (%s, %s) RETURNING player_id;", (name, role))
    pid = cur.fetchone()[0]
    conn.commit()
    print(f"Created player {pid} - {name}")


def add_team_to_tournament():
    cur.execute("""
        SELECT name, tournament_id, region FROM tournaments;
    """)
    tournaments = cur.fetchall()
    for i in range(len(tournaments)):
        print(f"{i + 1}) {tournaments[i][0]}")
    number = int(input("Enter tournament (use number from above): "))

    if tournaments[number - 1][2] != "Global":
        cur.execute("""
            SELECT name, team_id FROM teams WHERE region = %s;
        """, (tournaments[number - 1][2],))
        teams = cur.fetchall()
    else:
        cur.execute("""
            SELECT name, team_id FROM teams;
        """,)
        teams = cur.fetchall()

    for i in range(len(teams)):
        print(f"{i + 1}) {teams[i][0]}")
    team_id = teams[int(input(f"Select team from above to add to {tournaments[number - 1][0]}: ")) - 1][1]
    seeding = int(input(f"Enter the seeding placement: "))

    cur.execute("""
        INSERT INTO tournament_teams (tournament_id, team_id, seed)
        VALUES (%s, %s, %s)
    """, (tournaments[number - 1][1], team_id, seeding))
    conn.commit()
    print(f"Added team to tournament")

def add_player_map_stats(map_id, match_id, player_id, kills, deaths, assists, damage, healing, mitigated):
    cur.execute("""
        SELECT current_team_id FROM players WHERE player_id = %s;
    """, (player_id,))
    team_id = cur.fetchone()[0]

    fantasy_score = calculate_fantasy_score(kills, deaths, assists, damage, healing, mitigated)

    cur.execute("""
        INSERT INTO player_map_stats (map_played_id, player_id, team_id, eliminations, deaths, assists, damage, healing, mitigated, fantasy_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (map_id, player_id, team_id, kills, deaths, assists, damage, healing, mitigated, fantasy_score))

    cur.execute("SELECT maps_played FROM players WHERE player_id = %s;", (player_id,))
    career_maps_played = cur.fetchone()[0] or 0
    cur.execute("UPDATE players SET maps_played = %s WHERE player_id = %s;", (career_maps_played + 1, player_id))

    cur.execute("""
        INSERT INTO player_match_stats (match_id, player_id, team_id, number_maps_played, fantasy_score, eliminations, deaths, assists, damage, healing, mitigated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id, player_id) DO UPDATE
        SET number_maps_played = player_match_stats.number_maps_played + EXCLUDED.number_maps_played,
            fantasy_score = player_match_stats.fantasy_score + EXCLUDED.fantasy_score,
            eliminations = player_match_stats.eliminations + EXCLUDED.eliminations,
            deaths = player_match_stats.deaths + EXCLUDED.deaths,
            assists = player_match_stats.assists + EXCLUDED.assists,
            damage = player_match_stats.damage + EXCLUDED.damage,
            healing = player_match_stats.healing + EXCLUDED.healing,
            mitigated = player_match_stats.mitigated + EXCLUDED.mitigated;
    """, (match_id, player_id, team_id, 1, fantasy_score, kills, deaths, assists, damage, healing, mitigated))
    print(f"Added final map stats for player {player_id}")
    conn.commit()

def get_team_elos(team_id_a, team_id_b):
    cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (team_id_a,))
    a = cur.fetchone()
    cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (team_id_b,))
    b = cur.fetchone()
    a_elo = a[0] if a and a[0] is not None else 1000.0
    b_elo = b[0] if b and b[0] is not None else 1000.0
    return a_elo, b_elo

def get_map_winner(left_team_id, right_team_id, left_score, right_score):
    if left_score > right_score:
        return left_team_id
    elif right_score > left_score:
        return right_team_id
    return None  # draw

def complete_match(match_id, team_ids):
    # Fetch all map results
    cur.execute("""
        SELECT left_team_id, right_team_id, left_team_score, right_team_score
        FROM match_maps
        WHERE match_id = %s;
    """, (match_id,))
    maps = cur.fetchall()

    if not maps:
        print("No maps found for this match.")
        return

    wins = Counter()

    for left_id, right_id, left_score, right_score in maps:
        winner = get_map_winner(left_id, right_id, left_score, right_score)
        if winner:
            wins[winner] += 1

    team_a, team_b = team_ids
    count_team_a = wins.get(team_a, 0)
    count_team_b = wins.get(team_b, 0)

    if count_team_a > count_team_b:
        winning_team_id = team_a
        losing_team_id = team_b
    elif count_team_b > count_team_a:
        winning_team_id = team_b
        losing_team_id = team_a
    else:
        winning_team_id = None
        losing_team_id = None

    cur.execute("""
        UPDATE matches
        SET winner_id = %s,
            loser_id = %s,
            score_a = %s,
            score_b = %s
        WHERE match_id = %s;
    """, (winning_team_id, losing_team_id, count_team_a, count_team_b, match_id))

    # increment matches played
    for tid in team_ids:
        cur.execute("UPDATE teams SET matches_played = matches_played + 1 WHERE team_id = %s;", (tid,))

    cur.execute("SELECT player_id FROM player_match_stats WHERE match_id = %s;", (match_id,))
    for (pid,) in cur.fetchall():
        cur.execute("UPDATE players SET matches_played = matches_played + 1 WHERE player_id = %s;", (pid,))

    # --- TEAM ELO ---
    cur.execute("SELECT first_to FROM matches WHERE match_id = %s;", (match_id,))
    first_to = cur.fetchone()[0]

    if winning_team_id is None:
        actual_prob = 0.5
    else:
        min_maps = min(count_team_a, count_team_b)
        if first_to == 2:
            actual_prob = two_map_actual[str(min_maps)]
        elif first_to == 3:
            actual_prob = three_map_actual[str(min_maps)]
        elif first_to == 4:
            actual_prob = four_map_actual[str(min_maps)]
        else:
            actual_prob = 1

    if winning_team_id:
        cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (winning_team_id,))
        winning_team_old_elo = cur.fetchone()[0]
        cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (losing_team_id,))
        losing_team_old_elo = cur.fetchone()[0]

        update_team_elo(
            winning_team_old_elo,
            losing_team_old_elo,
            actual_prob,
            winning_team_id,
            losing_team_id,
            match_id
        )

    conn.commit()
    print(f"Completed match {match_id}")


def add_map():
    cur.execute(""" 
        SELECT m.match_id, m.date_played, ta.name AS team_a, tb.name AS team_b, m.team_a_id, m.team_b_id, m.match_type 
        FROM matches m 
        JOIN teams ta ON ta.team_id = m.team_a_id 
        JOIN teams tb ON tb.team_id = m.team_b_id 
        WHERE m.date_played BETWEEN NOW() - INTERVAL '1200 hours' AND NOW() + INTERVAL '1200 hours' ORDER BY m.date_played; 
    """)
    all_soon_games = cur.fetchall()
    for i in range(len(all_soon_games)):
        print(f"{i + 1}) {all_soon_games[i][2]} vs {all_soon_games[i][3]} | {all_soon_games[i][6]} match at {all_soon_games[i][1]}")
    match_idx = int(input("Which match above are we adding a map for (use the numbers): ")) - 1
    match_id = all_soon_games[match_idx][0]
    team_a_id = all_soon_games[match_idx][4]
    team_b_id = all_soon_games[match_idx][5]

    cur.execute("SELECT map_type_id, map_type FROM map_types;")
    map_types = cur.fetchall()
    for i, mt in enumerate(map_types):
        print(f"{i+1}) {mt[1]}")
    mt_idx = int(input("Map type: ")) - 1

    cur.execute("SELECT map_id, map_name FROM maps WHERE map_type_id = %s;", (map_types[mt_idx][0],))
    maps = cur.fetchall()
    for i, m in enumerate(maps):
        print(f"{i+1}) {m[1]}")
    map_id = maps[int(input("Map: ")) - 1][0]

    replay_code = input("Replay code: ")

    print("Which team was on LEFT side?")
    print(f"1) {all_soon_games[match_idx][2]}")
    print(f"2) {all_soon_games[match_idx][3]}")
    left_choice = int(input("Left team: "))

    if left_choice == 1:
        left_team_id = team_a_id
        right_team_id = team_b_id
    else:
        left_team_id = team_b_id
        right_team_id = team_a_id

    left_score = float(input("Left team score: "))
    right_score = float(input("Right team score: "))

    winner_id = get_map_winner(left_team_id, right_team_id, left_score, right_score)

    first_ban_team = int(input("Which team banned first: ")) + 1
    first_ban_team_id = all_soon_games[match_idx][first_ban_team + 2]
    cur.execute(""" 
        SELECT hero_id, hero_name FROM heroes; 
    """)
    all_heroes = cur.fetchall()
    for i in range(len(all_heroes)): print(f"{i + 1}) {all_heroes[i][1]}")
    first_hero_ban_id = all_heroes[int(input("Which hero was banned first: ")) - 1][0]
    second_hero_ban_id = all_heroes[int(input("Which hero was banned second: ")) - 1][0]

    cur.execute("""
        SELECT COUNT(*) FROM match_maps WHERE match_id = %s;
    """, (match_id,))
    map_number = cur.fetchone()[0] + 1

    cur.execute("""
        INSERT INTO match_maps (
            match_id, map_number, map_id, replay_code,
            left_team_id, right_team_id,
            left_team_score, right_team_score,
            winner_id, first_ban_team, first_ban, second_ban
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING match_map_id;
    """, (
        match_id, map_number, map_id, replay_code,
        left_team_id, right_team_id,
        left_score, right_score,
        winner_id, first_ban_team_id, first_hero_ban_id, second_hero_ban_id
    ))
    match_map_id = cur.fetchone()[0]

    stat_check = input("Add player stats? (y/n): ")
    if stat_check.lower() == "y":
        for i in range(10):
            player_name = input("Player name: ")
            kills = int(input("Kills: "))
            deaths = int(input("Deaths: "))
            assists = int(input("Assists: "))
            damage = int(input("Damage: "))
            healing = int(input("Healing: "))
            mitigated = int(input("Mitigated: "))

            cur.execute("SELECT player_id FROM players WHERE name = %s;", (player_name,))
            res = cur.fetchone()
            if not res:
                print("Player not found")
                continue

            add_player_map_stats(
                map_id=match_map_id,
                match_id=match_id,
                player_id=res[0],
                kills=kills,
                deaths=deaths,
                assists=assists,
                damage=damage,
                healing=healing,
                mitigated=mitigated
            )

    end_check = input("Final map of match? (y/n): ")
    if end_check.lower() == "y":
        complete_match(match_id, [team_a_id, team_b_id])

    conn.commit()


def player_transfer():
    name = input("Enter name of player: ")
    new_team = input("Enter their new team: ")

    cur.execute("""
         SELECT player_id FROM players WHERE name = %s;
     """, (name,))
    player_id = cur.fetchone()[0]

    if new_team != "Null":
        cur.execute("""
             SELECT team_id FROM teams WHERE name = %s;
         """, (new_team,))
        new_team_id = cur.fetchone()[0]
    else:
        new_team_id = None

    cur.execute("""
         SELECT current_team_id FROM players WHERE name = %s;
     """, (name,))
    old_team_id = cur.fetchone()[0]

    cur.execute("""
         INSERT INTO roster_changes (player_id, old_team_id, new_team_id)
         VALUES (%s, %s, %s);
     """, (player_id, old_team_id, new_team_id))

    cur.execute("""
         UPDATE players
         SET current_team_id = %s
         WHERE player_id = %s;
     """, (new_team_id, player_id))

    cur.execute("""
         UPDATE player_team_history
         SET end_date = NOW()
         WHERE history_id = (
             SELECT history_id
             FROM player_team_history
             WHERE player_id = %s AND team_id = %s
             ORDER BY history_id DESC
             LIMIT 1
         );
     """, (player_id, old_team_id))

    cur.execute("""
         INSERT INTO player_team_history (player_id, team_id, start_date)
         VALUES (%s, %s, NOW());
     """, (player_id, new_team_id))

    conn.commit()


# =========================
# Main CLI
# =========================
def cli():
    while True:
        print("\n=== MAIN MENU ===")
        print("1) Create Match")
        print("2) Create Team")
        print("3) Create Player")
        print("4) Add Map")
        print("5) Make transfer")
        print("6) Create tournament")
        print("7) Add team to tournament")
        print("8) COMPLETE MAP IN EMERGENCY")
        print("0) Quit")
        choice = input("Choose: ").strip()
        if choice == "1":
            create_match()
        elif choice == "2":
            create_team()
        elif choice == "3":
            create_player()
        elif choice == "4":
            add_map()
        elif choice == "5":
            player_transfer()
        elif choice == "6":
            name = input("Enter tournament name: ")
            location = input("Location of tournament (e.g. Online, Dreamhack Stockholm): ")
            region = input("Region of the tournament (NA/EMEA/KR/JP/PAC/ASIA): ")
            start_date = input("Starting date (YYYY-MM-DD): ")
            end_date = input("Ending date (YYYY-MM-DD): ")
            prize_pool = input("Enter prize pool (USD): ")
            cur.execute(
                "INSERT INTO tournaments (name, location, region, start_date, end_date, prize_pool) VALUES (%s,%s,%s,%s,%s,%s) RETURNING tournament_id;",
                (name, location, region, start_date or None, end_date or None, prize_pool or None ))
            conn.commit()
            tid = cur.fetchone()[0]
            print(f"Tournament created {tid}")

        elif choice == "7":
            add_team_to_tournament()
        elif choice == "8":
            m = int(input("Match ID to complete: ")); a = int(input("team A id: ")); b = int(input("team B id: "))
            complete_match(m, [a, b])
        elif choice == "0":
            print("Goodbye.")
            cur.close(); conn.close(); sys.exit(0)
        else:
            print("Invalid option.")

if __name__ == "__main__":
    cli()
