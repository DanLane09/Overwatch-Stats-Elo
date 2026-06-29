import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import cv2
import psycopg2
import bettercam
import easyocr
import pyautogui
pyautogui.FAILSAFE = True
import time
import numpy as np
import csv
import HeroAccumulator
import CropPositions
import LoadTemplates
import MatchTemplates
import ReadText
import main
from Experimental import time_model_training
import torch


# Initialize hardware-accelerated desktop capture and deep-learning OCR dependencies
camera = bettercam.create(output_color="RGB")
reader = easyocr.Reader(["en"], gpu=True)

conn = psycopg2.connect(host="localhost", port=5432, dbname="experiment_ow_stats_elo", user="postgres", password="pass")
cur = conn.cursor()


def check_white_pixels(image: np.ndarray, positions: list[list[int]]) -> bool:
    """
    Checks specified points to see if specific UI elements are active.
    Used for perks and ultimates.
    """
    return any((image[y, x] > 250).all() for x, y in positions)


def crop(image: np.ndarray, box: list[int]) -> np.ndarray:
    """
    Extracts a region from an image using [x_start, x_end, y_start, y_end] box limits.
    """
    x1, x2, y1, y2 = box
    return image[y1:y2, x1:x2]


def get_team(numb: int) -> str:
    """
    Maps player index numbers (0-9) to their respective teams (0-4: Blue, 5-9: Red).
    """
    return "blue" if numb < 5 else "red"


def get_player_data(image: np.ndarray, current_time: int, player_acc: HeroAccumulator.HeroAccumulator,
                    current_layout: CropPositions.PerkLayout, hero_templates: list[dict[str, np.ndarray]],
                    stat_templates, iteration, final):
    """
    Extracts and updates statistics for a single player from the current video frame.
    Runs for every player on every captured frame.
    """
    # Match hero portrait to templates to find currently selected hero
    # If not certain, revert to current hero as found in HeroAccumulator
    hero_name, hero_score = MatchTemplates.get_hero_name(
        img_crop=crop(image=image, box=current_layout.hero_crop[iteration]),
        templates=hero_templates)
    if hero_score < 0.5:
        hero_name = player_acc.get_current_hero()

    # Checking ultimate status by looking at specific pixel colour
    ult_charged = False
    if image[current_layout.ult_check[iteration][1],current_layout.ult_check[iteration][0]] > 250:
        ult_charged = True

    # Handling perk selection, with current_hero to reduce the number of templates to check
    minor_perk = None
    major_perk = None
    if current_layout.minor_perk_crop:
        maybe_minor_perk, minor_perk_score = MatchTemplates.get_perk(crop(image=image, box=current_layout.minor_perk_crop[iteration]), minor_perk_templates[hero_name])
        if minor_perk_score > 0.5:
            minor_perk = maybe_minor_perk
    if current_layout.major_perk_crop:
        maybe_major_perk, major_perk_score = MatchTemplates.get_perk(crop(image=image, box=current_layout.major_perk_crop[iteration]), major_perk_templates[hero_name])
        if major_perk_score > 0.5:
            major_perk = maybe_major_perk

    # Extracting numbers from the scoreboard
    numbers = {
        "eliminations": str(MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.eliminations_crop[iteration]), templates=stat_templates)),
        "assists": str(MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.assists_crop[iteration]), templates=stat_templates)),
        "deaths": str(MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.deaths_crop[iteration]), templates=stat_templates)),
        "damage": str(MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.damage_crop[iteration]), templates=stat_templates)),
        "healing": str(MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.healing_crop[iteration]), templates=stat_templates)),
        "mitigated": str(MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.mitigated_crop[iteration]), templates=stat_templates)),
    }

    # Fallback to previous values if matching returns empty results
    for key, value in numbers.items():
        if value == "":
            numbers[key] = player_acc.last_stats[key]
        else:
            numbers[key] = int(value)
            player_acc.last_stats[key] = numbers[key]

    # Initialise Snapshot object for this player on this timestep
    snap = HeroAccumulator.Snapshot(
            game_time=current_time,
            hero=hero_name,
            **numbers
    )

    # Only update the HeroAccumulator if the player has swapped heroes
    if hero_name != player_acc.get_current_hero() or final == True:
        player_acc.ingest(snap=snap)

    return [hero_name, player_acc.get_player_id(), ult_charged, minor_perk, major_perk, *numbers.values()]


def insert_hero_stats(conn, map_id, player_id, team_id, opp_id, hero_stats):
    """
    Inserts aggregated, hero-specific playtime metrics into the relational database store.
    """
    with conn.cursor() as cur:
        for hero, stats in hero_stats.items():
            # Resolve hero name labels to internal database primary identifiers
            cur.execute("""
                SELECT hero_id from heroes WHERE LOWER(REPLACE(REPLACE(REPLACE(hero_name, '.', ''), ':', ''), ' ', '_')) = %s;
            """, (hero, ))
            hero_id = cur.fetchone()[0]
            # Write finialised HeroAccumulators to database
            cur.execute("""
                INSERT INTO player_hero_map_stats (
                    player_id, map_played_id, team_id, opponent_id, hero_id, 
                    seconds_played, eliminations, assists, deaths, damage, healing, mitigated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
            , (
                player_id, map_id, team_id, opp_id, hero_id, stats["seconds"],
                stats["eliminations"], stats["assists"], stats["deaths"],
                stats["damage"], stats["healing"], stats["mitigated"]
            ))
    conn.commit()


def get_replays():
    """
    Finds the data required to parse maps_played (match_maps) that haven't been parsed.
    Ensures there is a replay code available for the map and that the game version is correct.
    """
    cur.execute("""
        SELECT 
            mp.match_id, 
            mp.map_played_id, 
            mp.replay_code, 
            mp.blue_team_id, 
            mp.red_team_id, 
            mp.blue_team_score, 
            mp.red_team_score
        FROM maps_played mp
        JOIN matches m ON mp.match_id = m.match_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM player_hero_map_stats phms
            WHERE phms.map_played_id = mp.map_played_id
        )
        AND mp.replay_code != '[null]'
        AND m.game_version = '2.22.1.1.149872'
        ORDER BY m.date_played, mp.match_id, mp.map_number;
    """)
    all_replays = cur.fetchall()
    return all_replays


# --- LOADING CORE TEMPLATES ---
role_templates = LoadTemplates.load_role_templates()
raw_hero_templates = LoadTemplates.load_hero_portrait_templates()
hero_templates = {"blue": {}, "red": {}}
for folder, templates in raw_hero_templates.items():
    parts = folder.split(" ")
    team, role = [part.lower().strip() for part in parts]
    hero_templates[team][role] = templates
minor_perk_templates = LoadTemplates.load_minor_perk_templates()
major_perk_templates = LoadTemplates.load_major_perk_templates()
stats_templates = LoadTemplates.load_stat_templates()

# --- MAIN PROCESSING LOOP ---
replays = get_replays()
print(len(replays))
print(replays)
time.sleep(5)
loaded_first_replay = False # Flag if we need to parse a replay already loaded into the client (importing won't work)
# Get replay codes specifically
for i in range (len(replays)):
    replay = replays[i]
    if i < len(replays) - 1:
        next_replay = replays[i + 1]
    else:
        next_replay = ["0"]
    all_data = []
    previous_time = 0
    previous_frame = None
    previous_layout = CropPositions.layouts["none"]
    # Initialise accumulators for all 10 players
    player_accs = [HeroAccumulator.HeroAccumulator() for _ in range(10)]

    # Automated UI interaction loop to import replay codes and handle errors
    if not loaded_first_replay:
        pyautogui.moveTo(1750, 335)
        pyautogui.leftClick()
        time.sleep(1)
        pyautogui.write(replay[2])
        pyautogui.moveTo(1040, 635)
        pyautogui.leftClick()
        time.sleep(3)
        replay_check = np.array(pyautogui.screenshot())
        # Check for errors when importing replay
        if replay_check[460, 1200][0] > 100:
            pyautogui.moveTo(960, 615)
            pyautogui.leftClick()
            if replay[0] != next_replay[0]:
                main.complete_match(match_id=replay[0], team_ids=[replay[3], replay[4]])
            continue
        pyautogui.moveTo(1025, 624)
        pyautogui.leftClick()
    temp = True
    print(f"Going in! {replay[2]}")
    time.sleep(15)
    game_running = True
    camera.start() # Start process to capture screenshots
    time.sleep(1)
    counter = 0
    while game_running:
        # Open scoreboard and wait for it to render fully before taking screenshot
        pyautogui.keyDown('tab')
        time.sleep(0.05)
        frame = camera.get_latest_frame()
        pyautogui.keyUp('tab')

        # FIRST FRAME INITIALIZATION: Resolve player names and look up database identity matches
        if previous_frame is None:
            screenshot = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
            for i in range(10):
                team_id = replay[3] if i < 5 else replay[4]
                name_img = crop(image=screenshot, box=CropPositions.layouts["none"].name_crop[i])
                player_id, player_name = ReadText.read_name(img_crop=name_img, team_id=team_id, reader=reader, cur=cur)
                player_accs[i].set_player_id(player_id)

        # END-GAME RECOGNITION: Detect completely dark pixels where we would expect to see light, indicating end of game
        if (frame[170, 810] < 30).all():
            game_running = False
            for i, acc in enumerate(player_accs):
                team = get_team(numb=i)
                role = acc.get_role()
                stats = get_player_data(image = previous_frame, current_time=previous_time, player_acc=acc, current_layout=previous_layout,
                                hero_templates=hero_templates[team][role], stat_templates=stats_templates, iteration=i, final=True)
                main.add_player_map_stats(map_id=replay[1], match_id=replay[0], player_id=acc.get_player_id(), kills=int(stats[5]),
                                        deaths=int(stats[7]), assists=int(stats[6]), damage=int(stats[8]), healing=int(stats[9]), mitigated=int(stats[10]))

            for i, acc in enumerate(player_accs):
                team_id = replay[4] if i > 4 else replay[3]
                opp_id = replay[4] if i < 5 else replay[3]
                insert_hero_stats(conn=conn, map_id=replay[1], player_id=acc.get_player_id(),
                                  team_id=team_id, opp_id=opp_id, hero_stats=acc.finalize())


        screenshot = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)

        # TIME PARSING: Use PyTorch model to track the match clock
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = time_model_training.TimerOCR()
        model.load_state_dict(torch.load("./ReadTimeModel.pth"))
        model.to(device).eval()
        timer = time_model_training.predict(model=model, image_path=crop(image=frame, box=CropPositions.time_crop))
        mins = timer[0:2]
        secs = timer[2:4]
        if mins == "" or secs == "": # The model cannot resolve a time
            #match_time = previous_time
            continue
        else:
            match_time = (int(mins) * 60) + int(secs) # Convert time to seconds
        if match_time == "":
            continue

        # LAYOUT DETECTION: Check perk positions to select the correct crops to use
        if check_white_pixels(frame, CropPositions.major_perk_positions):
            layout = CropPositions.layouts["major"]
        elif check_white_pixels(frame, CropPositions.minor_perk_positions):
            layout = CropPositions.layouts["minor"]
        else:
            layout = CropPositions.layouts["none"]

        temp_data = [match_time]
        for i, acc in enumerate(player_accs):
            team = get_team(numb=i)
            # Resolve role on initial frame
            if acc.get_role() == "none":
                role, score = MatchTemplates.get_role(crop(image=screenshot, box=layout.role_check[i]),
                                                      templates=role_templates[team])
                if score > 0.5:
                    acc.set_role(role)

            role = acc.get_role()
            # Error handling incase player doesn't select hero by the time tracking starts
            if role != "none":
                stats = get_player_data(image=screenshot, current_time=match_time, player_acc=acc, current_layout=layout,
                                    hero_templates=hero_templates[team][role], stat_templates=stats_templates,
                                    iteration=i, final=False)
            else:
                stats = [None, acc.get_player_id(), False, None, None, 0, 0, 0, 0, 0, 0]
            # Collating data to insert into CSV
            temp_data.append(stats)
            all_data.append([match_time] + stats)
            counter += 1

        previous_frame = screenshot
        previous_time = match_time
        previous_layout = layout
        print(temp_data)

    # Finishing matches in the database
    # Selecting winning team, updating elo, etc.
    if replay[0] != next_replay[0]:
        main.complete_match(match_id=replay[0], team_ids=[replay[3], replay[4]])

    camera.stop()

    # --- SAVE MAP DATA ---
    cur.execute("""
        SELECT name FROM teams WHERE team_id = %s;
    """, (replay[3],))
    first_name = cur.fetchone()[0]

    cur.execute("""
            SELECT name FROM teams WHERE team_id = %s;
        """, (replay[4],))
    second_name = cur.fetchone()[0]

    with open(f'./Game CSVs/{first_name} vs {second_name} --- match_id-{replay[0]}, map_played_id-{replay[1]}.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(
            ['time', 'hero', 'player_id', 'ult_charged', 'minor_perk', 'major_perk', 'eliminations', 'assists', 'deaths', 'damage', 'healing', 'mitigated'])
        writer.writerows(all_data)

    conn.commit()
    pyautogui.press("esc") # Exit current replay and go back to career profile
    time.sleep(10)