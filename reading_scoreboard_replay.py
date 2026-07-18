import os
from typing import Tuple, Any
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

def get_pixel(image: np.ndarray, position: list[int]) -> int:
    x, y = position
    return image[y, x]

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


def get_team_scores(frame: np.ndarray, colour_frame:np.ndarray, time: int, game_mode: str, blue_points_captured: int,
                    red_points_captured: int, blue_distance: float, red_distance: float, in_control: str|None, current_point: str|None) -> Tuple[int, int, float, float, str|None, str|None]:
    if (game_mode == "Escort") or (game_mode == "Hybrid"):
        if (get_pixel(colour_frame, CropPositions.escort["overtime_layout_check"]) != [255, 165, 0]).all():
            blue_team_score, red_team_score = MatchTemplates.get_escort_score(frame, CropPositions.escort["points"],
                                                                          escort_score_templates)
            blue_capture_distance = MatchTemplates.read_stat(crop(frame, CropPositions.escort["distance"]["blue_distance"]), stats_templates)
            red_capture_distance = MatchTemplates.read_stat(crop(frame, CropPositions.escort["distance"]["red_distance"]), stats_templates)
        else:
            blue_team_score = blue_points_captured
            red_team_score = red_points_captured
            blue_capture_distance = MatchTemplates.read_stat(crop(frame, CropPositions.escort["distance"]["overtime_blue_distance"]), stats_templates)
            red_capture_distance = MatchTemplates.read_stat(crop(frame, CropPositions.escort["distance"]["overtime_red_distance"]), stats_templates)
        if blue_capture_distance != "":
            blue_capture_distance = float(blue_capture_distance[:-2] + "." + blue_capture_distance[-2:])
        else:
            blue_capture_distance = blue_distance
        if red_capture_distance != "":
            red_capture_distance = float(red_capture_distance[:-2] + "." + red_capture_distance[-2:])
        else:
            red_capture_distance = red_distance
        if (blue_capture_distance == 0.1 and red_capture_distance == 0.1) or (blue_capture_distance == 0.0 and red_capture_distance == 0.1):
            blue_capture_distance, red_capture_distance = blue_distance, red_distance
        print(blue_capture_distance, red_capture_distance)
        if (blue_points_captured < blue_team_score) and (blue_team_score - blue_points_captured == 1):
            print(f"Blue team captured point {blue_team_score} at {time}")
            blue_points_captured = blue_team_score
        if (red_points_captured < red_team_score) and (red_team_score - red_points_captured == 1):
            print(f"Red team captured point {red_team_score} at {time}")
            red_points_captured = red_team_score
        return int(blue_points_captured), int(red_points_captured), blue_capture_distance, red_capture_distance, in_control, current_point

    elif game_mode == "Control":
        # Setting default values
        score_layout = CropPositions.control["in_game"]
        percentage_layout = CropPositions.control["percentage"]
        team_in_control_check = CropPositions.control["in_game_control_check"]

        # Indicates between rounds or round has started but objective hasn't unlocked yet
        if ((get_pixel(colour_frame, CropPositions.control["pre_point_layout_check"][0]) == [0, 190, 255]).all() and
           (get_pixel(colour_frame, CropPositions.control["pre_point_layout_check"][1]) == [239, 46, 81]).all()):
            in_control = None
            current_point = None
            return blue_points_captured, red_points_captured, 0, 0, in_control, current_point

        # Indicates that overtime is active
        if (get_pixel(colour_frame, CropPositions.control["overtime_layout_check"]) == [255, 165, 0]).all():
            percentage_layout = CropPositions.control["overtime_percentage"]
            team_in_control_check = CropPositions.control["overtime_control_check"]

        # Blue team is in control
        if ((get_pixel(colour_frame, team_in_control_check[0]) == [255, 255, 255]).all() and
            (get_pixel(colour_frame, team_in_control_check[1]) == [239, 46, 81]).all() and
            (in_control != "blue")):
                blue_new_distance, red_new_distance = MatchTemplates.get_control_percentage(img=colour_frame,
                                                                                    crop_positions=percentage_layout,
                                                                                    templates=percentage_templates,
                                                                                    targets=[[255, 255, 255], [239, 46, 81]])
                if blue_new_distance == "" or red_new_distance == "":
                    return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point
                print(f"Blue team has captured the objective at {time}")
                print(f"Blue capture progress: {blue_distance}, Red capture progress: {red_new_distance}")
                in_control = "blue"
                return blue_points_captured, red_points_captured, blue_distance, red_new_distance, in_control, current_point

        # Red team is in control
        elif ((get_pixel(colour_frame, team_in_control_check[0]) == [0, 190, 255]).all() and
              (get_pixel(colour_frame, team_in_control_check[1]) == [255, 255, 255]).all() and
              (in_control != "red")):
                blue_new_distance, red_new_distance = MatchTemplates.get_control_percentage(img=colour_frame,
                                                                                    crop_positions=percentage_layout,
                                                                                    templates=percentage_templates,
                                                                                    targets=[[0, 190, 255], [255, 255, 255]])
                if blue_new_distance == "" or red_new_distance == "":
                    return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point
                print(f"Red team has captured the objective at {time}")
                print(f"Blue capture progress: {blue_new_distance}, Red capture progress: {red_distance}")
                in_control = "red"
                return blue_points_captured, red_points_captured, blue_new_distance, red_distance, in_control, current_point

        # Neither team is in control
        # Triggered right after the point unlocks or at the end of the round before the UI changes to next round
        elif ((get_pixel(colour_frame, team_in_control_check[0]) == [0, 190, 255]).all() and
           (get_pixel(colour_frame, team_in_control_check[1]) == [239, 46, 81]).all()):
            blue_team_score, red_team_score = MatchTemplates.get_control_score(colour_frame, score_layout,
                                                                               control_score_templates,
                                                                               [[0, 190, 255], [239, 46, 81]])
            blue_new_distance, red_new_distance = MatchTemplates.get_control_percentage(img=colour_frame,
                                                                                crop_positions=percentage_layout,
                                                                                templates=percentage_templates,
                                                                                targets=[[0, 190, 255], [239, 46, 81]])
            if blue_new_distance == "" or red_new_distance == "":
                return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point
            point = current_point
            if current_point is None:
                point = MatchTemplates.get_control_point(img=colour_frame, crop_positions=CropPositions.control["point_selection"], templates=control_point_templates)
                if point is not None:
                    print(f"Control point {point} unlocked")

            if (blue_team_score - blue_points_captured == 1) or (red_team_score - red_points_captured == 1):
                print(blue_team_score, red_team_score)

            if in_control == "blue":
                in_control = None
                print(f"Blue team has won point {point}")
                return blue_team_score, red_team_score, 100, red_new_distance, in_control, point
            elif in_control == "red":
                in_control = None
                print(f"Red team has won point {point}")
                return blue_team_score, red_team_score, blue_new_distance, 100, in_control, point
            else:
                return blue_team_score, red_team_score, blue_new_distance, red_new_distance, in_control, point

    elif game_mode == "Flashpoint":
        # Setting default values
        score_layout = CropPositions.flashpoint["in_game"]
        percentage_layout = CropPositions.flashpoint["percentage"]
        team_in_control_check = CropPositions.flashpoint["in_game_control_check"]

        # Indicates between rounds or round has started but objective hasn't unlocked yet
        if ((get_pixel(colour_frame, CropPositions.flashpoint["pre_point_layout_check"][0]) == [0, 190, 255]).all() and
                (get_pixel(colour_frame, CropPositions.flashpoint["pre_point_layout_check"][1]) == [239, 46, 81]).all()):
            if in_control is not None:
                score_layout = CropPositions.flashpoint["pre_point"]
                blue_team_score, red_team_score = MatchTemplates.get_control_score(colour_frame, score_layout,
                                                                                   flashpoint_score_templates,
                                                                                   [[0, 190, 255], [239, 46, 81]])

                if (blue_team_score - blue_points_captured == 1) or (red_team_score - red_points_captured == 1):
                    print(blue_team_score, red_team_score)

                if in_control == "blue":
                    in_control = None
                    print(f"Blue team has won point {current_point}")
                    return blue_team_score, red_team_score, 100, red_distance, in_control, current_point
                elif in_control == "red":
                    in_control = None
                    print(f"Red team has won point {current_point}")
                    return blue_team_score, red_team_score, blue_distance, 100, in_control, current_point
            in_control = None
            current_point = None
            return blue_points_captured, red_points_captured, 0, 0, in_control, current_point

        # Indicates that overtime is active
        if (get_pixel(colour_frame, CropPositions.flashpoint["overtime_layout_check"]) == [255, 165, 0]).all():
            percentage_layout = CropPositions.flashpoint["overtime_percentage"]
            team_in_control_check = CropPositions.flashpoint["overtime_control_check"]

        # Blue team is in control
        if ((get_pixel(colour_frame, team_in_control_check[0]) == [255, 255, 255]).all() and
                (get_pixel(colour_frame, team_in_control_check[1]) == [239, 46, 81]).all() and
                (in_control != "blue")):
            blue_new_distance, red_new_distance = MatchTemplates.get_control_percentage(img=colour_frame,
                                                                                        crop_positions=percentage_layout,
                                                                                        templates=percentage_templates,
                                                                                        targets=[[255, 255, 255],
                                                                                                 [239, 46, 81]])
            if blue_new_distance == "" or red_new_distance == "":
                return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point
            print(f"Blue team has captured the objective at {time}")
            print(f"Blue capture progress: {blue_distance}, Red capture progress: {red_new_distance}")
            in_control = "blue"
            return blue_points_captured, red_points_captured, blue_distance, red_new_distance, in_control, current_point

        # Red team is in control
        elif ((get_pixel(colour_frame, team_in_control_check[0]) == [0, 190, 255]).all() and
              (get_pixel(colour_frame, team_in_control_check[1]) == [255, 255, 255]).all() and
              (in_control != "red")):
            blue_new_distance, red_new_distance = MatchTemplates.get_control_percentage(img=colour_frame,
                                                                                        crop_positions=percentage_layout,
                                                                                        templates=percentage_templates,
                                                                                        targets=[[0, 190, 255],
                                                                                                 [255, 255, 255]])
            if blue_new_distance == "" or red_new_distance == "":
                return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point
            print(f"Red team has captured the objective at {time}")
            print(f"Blue capture progress: {blue_new_distance}, Red capture progress: {red_distance}")
            in_control = "red"
            return blue_points_captured, red_points_captured, blue_new_distance, red_distance, in_control, current_point

        # Neither team is in control
        # Triggered right after the point unlocks or at the end of the round before the UI changes to next round
        elif ((get_pixel(colour_frame, team_in_control_check[0]) == [0, 190, 255]).all() and
              (get_pixel(colour_frame, team_in_control_check[1]) == [239, 46, 81]).all()):
            blue_team_score, red_team_score = MatchTemplates.get_control_score(colour_frame, score_layout,
                                                                               flashpoint_score_templates,
                                                                               [[0, 190, 255], [239, 46, 81]])
            blue_new_distance, red_new_distance = MatchTemplates.get_control_percentage(img=colour_frame,
                                                                                        crop_positions=percentage_layout,
                                                                                        templates=percentage_templates,
                                                                                        targets=[[0, 190, 255],
                                                                                                 [239, 46, 81]])
            if blue_new_distance == "" or red_new_distance == "":
                return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point
            point = current_point
            if current_point is None:
                point = MatchTemplates.get_control_point(img=colour_frame,
                                                         crop_positions=CropPositions.flashpoint["point_selection"],
                                                         templates=flashpoint_point_templates)
                if point is not None:
                    print(f"Control point {point} unlocked")

            if (blue_team_score - blue_points_captured == 1) or (red_team_score - red_points_captured == 1):
                print(blue_team_score, red_team_score)

            if in_control == "blue":
                in_control = None
                print(f"Blue team has won point {point}")
                return blue_team_score, red_team_score, 100, red_new_distance, in_control, point
            elif in_control == "red":
                in_control = None
                print(f"Red team has won point {point}")
                return blue_team_score, red_team_score, blue_new_distance, 100, in_control, point
            else:
                return blue_team_score, red_team_score, blue_new_distance, red_new_distance, in_control, point

    elif game_mode == "Push":
        distances = []
        crop_positions = CropPositions.push["in_game"]
        if (get_pixel(colour_frame, CropPositions.push["overtime_layout_check"]) == [255, 166, 0]).all():
            crop_positions = CropPositions.push["overtime"]
        if time <= 30:
            print(0.0, 0.0)
            return 0, 0, 0.0, 0.0, in_control, current_point
        for i, value in enumerate(crop_positions.values()):
            cropped_img = crop(frame, value)
            if i < 2:
                _, binary = cv2.threshold(cropped_img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                number = MatchTemplates.read_number(image=binary, templates=percentage_templates)
            else:
                _, binary = cv2.threshold(cropped_img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                number = MatchTemplates.read_number(image=binary, templates=push_decimal_templetes, threshold=0.8, iou_threshold=0.4)
            if number != "":
                distances.append(int(number))
            else:
                distances.append(0)
        blue_return = float(f"{distances[0]}.{distances[2]}")
        red_return = float(f"{distances[1]}.{distances[3]}")
        if (blue_return > blue_distance + 4) or (red_return > red_distance + 4):
            print(blue_distance, red_distance)
            return 0, 0, blue_distance, red_distance, in_control, current_point

        print(blue_return, red_return)
        return 0, 0, blue_return, red_return, in_control, current_point


    return blue_points_captured, red_points_captured, blue_distance, red_distance, in_control, current_point


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
            mp.red_team_score,
            mt.map_type
        FROM maps_played mp
        JOIN matches m ON mp.match_id = m.match_id
        JOIN maps maps ON mp.map_id = maps.map_id
        JOIN map_types mt ON maps.map_type_id = mt.map_type_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM player_hero_map_stats phms
            WHERE phms.map_played_id = mp.map_played_id
        )
        AND mp.replay_code != '[null]'
        AND m.game_version = '2.23.0.0.150818'
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
escort_score_templates = LoadTemplates.load_escort_score_templates()
control_score_templates = LoadTemplates.load_control_score_templates()
flashpoint_score_templates = LoadTemplates.load_flashpoint_score_templates()
percentage_templates = LoadTemplates.load_percentage_templates()
control_point_templates = LoadTemplates.load_control_point_templates()
flashpoint_point_templates = LoadTemplates.load_flashpoint_point_templates()
push_decimal_templetes = LoadTemplates.load_push_decimals()

# --- MAIN PROCESSING LOOP ---
replays = get_replays()
print(len(replays))
print(replays)
time.sleep(5)
loaded_first_replay = True # Flag if we need to parse a replay already loaded into the client (importing won't work)
# Get replay codes specifically
for i in range (len(replays)):
    replay = replays[i]
    if i < len(replays) - 1:
        next_replay = replays[i + 1]
    else:
        next_replay = ["0"]
    all_data = []
    previous_time = 0
    previous_scoreboard_frame = None
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
    loaded_first_replay = False
    print(f"Going in! {replay[2]}")
    time.sleep(15)
    game_running = True
    camera.start() # Start process to capture screenshots
    time.sleep(1)
    counter = 0

    blue_team_points_captured = 0
    red_team_points_captured = 0
    blue_team_capture_distance = 0
    red_team_capture_distance = 0
    in_control = None
    current_point = None

    while game_running:
        game_frame = camera.get_latest_frame()
        # Open scoreboard and wait for it to render fully before taking screenshot
        pyautogui.keyDown('tab')
        time.sleep(0.05)
        scoreboard_frame = camera.get_latest_frame()
        pyautogui.keyUp('tab')

        # FIRST FRAME INITIALIZATION: Resolve player names and look up database identity matches
        if previous_scoreboard_frame is None:
            gray_scoreboard = cv2.cvtColor(scoreboard_frame, cv2.COLOR_RGB2GRAY)
            for i in range(10):
                team_id = replay[3] if i < 5 else replay[4]
                name_img = crop(image=gray_scoreboard, box=CropPositions.layouts["none"].name_crop[i])
                player_id, player_name = ReadText.read_name(img_crop=name_img, team_id=team_id, reader=reader, cur=cur)
                player_accs[i].set_player_id(player_id)

        # END-GAME RECOGNITION: Detect completely dark pixels where we would expect to see light, indicating end of game
        if (scoreboard_frame[170, 810] < 30).all():
            game_running = False
            for i, acc in enumerate(player_accs):
                team = get_team(numb=i)
                role = acc.get_role()
                stats = get_player_data(image = previous_scoreboard_frame, current_time=previous_time, player_acc=acc,
                                        current_layout=previous_layout,hero_templates=hero_templates[team][role],
                                        stat_templates=stats_templates, iteration=i, final=True)
                main.add_player_map_stats(map_id=replay[1], match_id=replay[0], player_id=acc.get_player_id(),
                                          kills=int(stats[5]),deaths=int(stats[7]), assists=int(stats[6]),
                                          damage=int(stats[8]), healing=int(stats[9]), mitigated=int(stats[10]))

            for i, acc in enumerate(player_accs):
                team_id = replay[4] if i > 4 else replay[3]
                opp_id = replay[4] if i < 5 else replay[3]
                insert_hero_stats(conn=conn, map_id=replay[1], player_id=acc.get_player_id(),
                                  team_id=team_id, opp_id=opp_id, hero_stats=acc.finalize())

        gray_game_frame = cv2.cvtColor(game_frame, cv2.COLOR_RGB2GRAY)
        gray_scoreboard = cv2.cvtColor(scoreboard_frame, cv2.COLOR_RGB2GRAY)
        # TIME PARSING: Use PyTorch model to track the match clock
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = time_model_training.TimerOCR()
        model.load_state_dict(torch.load("./ReadTimeModel.pth"))
        model.to(device).eval()
        timer = time_model_training.predict(model=model, image_path=crop(image=scoreboard_frame,
                                                                         box=CropPositions.time_crop))
        mins = timer[0:2]
        secs = timer[2:4]
        if mins == "" or secs == "": # The model cannot resolve a time
            # Still need to check team scores. At end of game, team scores update while time doesn't show so we use previous_time
            (blue_team_points_captured,
             red_team_points_captured,
             blue_team_capture_distance,
             red_team_capture_distance,
             in_control, current_point) = get_team_scores(frame=gray_game_frame, colour_frame=game_frame, time=previous_time,
                                           game_mode=replay[7], blue_points_captured=blue_team_points_captured,
                                           red_points_captured=red_team_points_captured,
                                           blue_distance=blue_team_capture_distance,
                                           red_distance=red_team_capture_distance, in_control=in_control, current_point=current_point)
            continue
        else:
            match_time = (int(mins) * 60) + int(secs) # Convert time to seconds

        # Get the number of points captured by each team
        (blue_team_points_captured,
         red_team_points_captured,
         blue_team_capture_distance,
         red_team_capture_distance,
         in_control, current_point) = get_team_scores(frame=gray_game_frame, colour_frame=game_frame, time=match_time,
                                       game_mode=replay[7], blue_points_captured=blue_team_points_captured,
                                       red_points_captured=red_team_points_captured,
                                       blue_distance=blue_team_capture_distance,
                                        red_distance=red_team_capture_distance, in_control=in_control, current_point=current_point)

        # LAYOUT DETECTION: Check perk positions to select the correct crops to use
        if check_white_pixels(scoreboard_frame, CropPositions.major_perk_positions):
            layout = CropPositions.layouts["major"]
        elif check_white_pixels(scoreboard_frame, CropPositions.minor_perk_positions):
            layout = CropPositions.layouts["minor"]
        else:
            layout = CropPositions.layouts["none"]

        temp_data = [match_time]
        for i, acc in enumerate(player_accs):
            team = get_team(numb=i)
            # Resolve role on initial scoreboard frame
            if acc.get_role() == "none":
                role, score = MatchTemplates.get_role(crop(image=gray_scoreboard, box=layout.role_check[i]),
                                                      templates=role_templates[team])
                if score > 0.5:
                    acc.set_role(role)

            role = acc.get_role()
            # Error handling incase player doesn't select hero by the time tracking starts
            if role != "none":
                stats = get_player_data(image=gray_scoreboard, current_time=match_time, player_acc=acc,
                                        current_layout=layout, hero_templates=hero_templates[team][role],
                                        stat_templates=stats_templates, iteration=i, final=False)
            else:
                stats = [None, acc.get_player_id(), False, None, None, 0, 0, 0, 0, 0, 0]
            # Collating data to insert into CSV
            temp_data.append(stats)
            all_data.append([match_time] + stats)
            counter += 1

        previous_scoreboard_frame = gray_scoreboard
        previous_colour_frame = game_frame
        previous_time = match_time
        previous_layout = layout
        #print(temp_data)

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