import psycopg2.extras
from collections import Counter, defaultdict
import sys


conn = psycopg2.connect(host="localhost", port=5432, dbname="testing", user="postgres", password="pass")
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
    version = input("Enter game version: ")
    version = "2.20.1.0.145326"

    if "." in offset:
        hours, minutes = offset.split(".")
        minutes = int(float("0." + minutes) * 60)
        offset = f"{int(hours):+03d}:{minutes:02d}"
    else:
        offset = f"{int(offset):+03d}"
    timestamp_with_offset = f"{date_str}{offset}"

    cur.execute("""
        INSERT INTO matches (date_played, tournament_id, match_type, game_version, best_of, team_a_id, team_b_id)
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

def add_player_map_stats(map_id, match_id):
    for i in range(10):
        player_name = input("Enter player name: ")
        kills = int(input("Enter number of kills: "))
        deaths = int(input("Enter number of deaths: "))
        assists = int(input("Enter number of assists: "))
        damage = int(input("Enter number of damage: "))
        healing = int(input("Enter number of healing: "))
        mitigated = int(input("Enter number of mitigated: "))

        cur.execute("SELECT player_id, current_team_id FROM players WHERE name = %s;", (player_name,))
        res = cur.fetchone()
        if not res:
            print(f"Player {player_name} not found — skipping.")
            continue
        player_id, team_id = res

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
    conn.commit()

def get_team_elos(team_id_a, team_id_b):
    cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (team_id_a,))
    a = cur.fetchone()
    cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (team_id_b,))
    b = cur.fetchone()
    a_elo = a[0] if a and a[0] is not None else 1000.0
    b_elo = b[0] if b and b[0] is not None else 1000.0
    return a_elo, b_elo

# We keep your complete_match largely intact but add call to propagate_result at the end.
def complete_match(match_id, team_ids):
    cur.execute("SELECT winner_id FROM match_maps WHERE match_id = %s;", (match_id,))
    data = cur.fetchall()
    if not data:
        print("No maps found for this match.")
        return
    winner_ids = [r[0] for r in data]
    winning_team_id = Counter(winner_ids).most_common()[0][0]
    counts = Counter(winner_ids)
    count_team_a = counts.get(team_ids[0], 0)
    count_team_b = counts.get(team_ids[1], 0)

    cur.execute("""
        SELECT CASE WHEN %s = team_a_id THEN team_b_id ELSE team_a_id END AS loser_id
        FROM matches
        WHERE match_id = %s;
    """, (winning_team_id, match_id))
    losing_team_id = cur.fetchone()[0]

    cur.execute("""
        UPDATE matches
        SET winner_id = %s,
            loser_id = %s,
            score_a = %s,
            score_b = %s
        WHERE match_id = %s;
    """, (winning_team_id, losing_team_id, count_team_a, count_team_b, match_id))

    for i in range(2):
        cur.execute("UPDATE teams SET matches_played = matches_played + 1 WHERE team_id = %s;", (team_ids[i],))

    cur.execute("SELECT player_id FROM player_match_stats WHERE match_id = %s;", (match_id,))
    all_player_ids = [r[0] for r in cur.fetchall()]
    for pid in all_player_ids:
        cur.execute("UPDATE players SET matches_played = COALESCE(matches_played,0) + 1 WHERE player_id = %s;", (pid,))

    cur.execute("SELECT best_of FROM matches WHERE match_id = %s;", (match_id,)) # CHANGE TO 'first_to' #######################################################
    first_to_row = cur.fetchone()
    first_to = first_to_row[0] if first_to_row else None

    if first_to == 2:
        actual_prob = two_map_actual[f"{min(count_team_a, count_team_b)}"]
    elif first_to == 3:
        actual_prob = three_map_actual[f"{min(count_team_a, count_team_b)}"]
    elif first_to == 4:
        actual_prob = four_map_actual[f"{min(count_team_a, count_team_b)}"]
    else:
        actual_prob = 1 if winning_team_id else 0

    cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (winning_team_id,))
    winning_team_old_elo = cur.fetchone()[0]
    cur.execute("SELECT elo FROM teams WHERE team_id = %s;", (losing_team_id,))
    losing_team_old_elo = cur.fetchone()[0]

    cur.execute("""
        SELECT pms.player_id, pms.team_id, pms.eliminations, pms.deaths, pms.assists, pms.damage, pms.healing, pms.mitigated, pms.fantasy_score
        FROM player_match_stats pms
        WHERE pms.match_id = %s;
    """, (match_id,))
    player_rows = cur.fetchall()

    team_players = defaultdict(list)
    player_stats = {}
    for (player_id, team_id, eliminations, deaths, assists, damage, healing, mitigated, fantasy_score) in player_rows:
        eliminations = eliminations or 0; deaths = deaths or 0; assists = assists or 0
        damage = damage or 0; healing = healing or 0; mitigated = mitigated or 0
        fantasy_score = fantasy_score or 0
        player_stats[player_id] = {"team_id": team_id, "eliminations": eliminations, "deaths": deaths, "assists": assists,
                                   "damage": damage, "healing": healing, "mitigated": mitigated, "fantasy_score": fantasy_score}
        team_players[team_id].append(player_id)

    team_a_elo, team_b_elo = get_team_elos(team_ids[0], team_ids[1])
    team_elo = {team_ids[0]: team_a_elo, team_ids[1]: team_b_elo}

    cps_raw = {}
    role_cps_raw = {}
    team_cps_sum = defaultdict(float)
    team_player_count = defaultdict(int)

    for pid, stats in player_stats.items():
        norm = get_normalized_match_stats(match_id, pid)
        if not norm:
            continue
        cps = calculate_player_cps(norm["eliminations"], norm["deaths"], norm["assists"], norm["damage"], norm["healing"], norm["mitigated"], norm["fantasy"])
        tid = stats["team_id"]
        team_cps_sum[tid] += cps
        team_player_count[tid] += 1
        cur.execute("SELECT role FROM players WHERE player_id = %s;", (pid,))
        role_row = cur.fetchone()
        role = role_row[0] if role_row else None
        r_cps = calculate_role_cps(norm["eliminations"], norm["deaths"], norm["assists"], norm["damage"], norm["healing"], norm["mitigated"], role)
        role_cps_raw[pid] = (r_cps, role)

    team_avg_cps = {}
    for tid in team_players.keys():
        if team_player_count[tid] > 0:
            team_avg_cps[tid] = team_cps_sum[tid] / team_player_count[tid]
        else:
            team_avg_cps[tid] = 0.0

    for pid, stats in player_stats.items():
        tid = stats["team_id"]
        opp_tid = team_ids[0] if tid == team_ids[1] else team_ids[1]
        player_team_elo = team_elo.get(tid, 1000.0)
        opponent_team_elo = team_elo.get(opp_tid, 1000.0)
        odf = (opponent_team_elo / player_team_elo) if (player_team_elo and player_team_elo > 0) else 1.0
        team_avg = team_avg_cps.get(tid, 0.0)
        # note: cps_raw may be empty - set to role value to avoid crash
        cps_raw_val = calculate_player_cps(stats["eliminations"], stats["deaths"], stats["assists"], stats["damage"], stats["healing"], stats["mitigated"], stats["fantasy_score"])
        if team_avg and team_avg != 0:
            tba = cps_raw_val / team_avg
        else:
            tba = 1.0
        r_cps_val, role = role_cps_raw[pid]
        role_adjusted = r_cps_val * odf * tba
        cur.execute("SELECT elo FROM players WHERE player_id = %s;", (pid,))
        row = cur.fetchone()
        old_player_elo = row[0] if row else 1000.0
        new_player_elo = old_player_elo + role_adjusted
        cur.execute("UPDATE players SET elo = %s WHERE player_id = %s;", (new_player_elo, pid))
        cur.execute("INSERT INTO elo_history (entity_type, entity_id, old_elo, new_elo, match_id) VALUES (%s, %s, %s, %s, %s)",
                    ("player", pid, old_player_elo, new_player_elo, match_id))

    update_team_elo(winning_team_old_elo, losing_team_old_elo, actual_prob, winning_team_id, losing_team_id, match_id)

    conn.commit()
    print(f"Completed match {match_id}: updated player and team elos.")


def add_map():
    cur.execute("""
        SELECT m.match_id, m.date_played, ta.name AS team_a, tb.name AS team_b, m.team_a_id, m.team_b_id, m.match_type
        FROM matches m
        JOIN teams ta ON ta.team_id = m.team_a_id
        JOIN teams tb ON tb.team_id = m.team_b_id
        WHERE m.date_played BETWEEN
        NOW() - INTERVAL '12 hours'
        AND NOW() + INTERVAL '12 hours'
        ORDER BY m.date_played;
    """)
    all_soon_games = cur.fetchall()
    for i in range(len(all_soon_games)):
        print(f"{i + 1}) {all_soon_games[i][2]} vs {all_soon_games[i][3]} | {all_soon_games[i][6]} match at {all_soon_games[i][1]}")
    match_idx = int(input("Which match above are we adding a map for (use the numbers): ")) - 1
    match_id = all_soon_games[match_idx][0]
    team_ids = [all_soon_games[match_idx][4], all_soon_games[match_idx][5]]

    cur.execute("""
        SELECT map_type_id, map_type FROM map_types;
    """)
    all_map_types = cur.fetchall()
    for i in range(len(all_map_types)):
        print(f"{i + 1}) {all_map_types[i][1]}")
    map_type_idx = int(input("Enter map type: ")) - 1
    cur.execute("""
            SELECT map_id, map_name FROM maps 
            WHERE map_type_id = %s;
        """, (all_map_types[map_type_idx][0],))
    all_maps = cur.fetchall()
    for i in range(len(all_maps)):
        print(f"{i + 1}) {all_maps[i][1]}")
    playing_map_id = all_maps[int(input("Enter map: ")) - 1][0]

    replay_code = str(input("Enter replay code: "))

    team_b_score = int(input("What was the score of the team that attacked first/were on the right of the overview: "))
    team_a_score = int(input("What was the score of the team that defended first/were on the left of the overview: "))

    for i in range(2):
        print(f"{i + 1}) {all_soon_games[match_idx][i + 2]}")
    winner = int(input("Which was the winning team: ")) + 1
    winner_id = all_soon_games[match_idx][winner + 2]
    first_ban_team = int(input("Which team banned first: ")) + 1
    first_ban_team_id = all_soon_games[match_idx][first_ban_team + 2]



    cur.execute("""
        SELECT hero_id, hero_name FROM heroes;
    """)
    all_heroes = cur.fetchall()
    for i in range(len(all_heroes)):
        print(f"{i + 1}) {all_heroes[i][1]}")
    first_hero_ban_id = all_heroes[int(input("Which hero was banned first: ")) - 1][0]
    second_hero_ban_id = all_heroes[int(input("Which hero was banned second: ")) - 1][0]

    cur.execute("""
        SELECT COUNT(*) AS occurrences FROM match_maps WHERE match_id = %s GROUP BY match_id;
    """, (all_soon_games[match_idx][0],))
    numb_prev_maps = cur.fetchone()
    if numb_prev_maps is None:
        numb_prev_maps = 0
    else:
        numb_prev_maps = numb_prev_maps[0]

    cur.execute("""
        INSERT INTO match_maps (match_id, map_number, map_id, replay_code, team_a_score, team_b_score, winner_id, first_ban_team, first_ban, second_ban)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING match_map_id;
    """, (match_id, numb_prev_maps + 1, playing_map_id, replay_code, team_a_score, team_b_score, winner_id, first_ban_team_id, first_hero_ban_id, second_hero_ban_id))
    match_map_id = cur.fetchone()[0]


    for i in range(2):
        cur.execute("""
            UPDATE teams
            SET maps_played = %s
            WHERE team_id = %s;
        """, (numb_prev_maps + 1, team_ids[i]))

    stat_check = input("Do you have stats to input (y/n): ")
    if stat_check == "y":
        add_player_map_stats(match_map_id, match_id)

    end_check = input("Was that the final map (y/n): ")
    if end_check == "y":
        complete_match(match_id, team_ids)

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
