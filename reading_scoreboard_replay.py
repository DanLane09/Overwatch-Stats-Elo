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


camera = bettercam.create(output_color="RGB")
reader = easyocr.Reader(["en"], gpu=True)

conn = psycopg2.connect(host="localhost", port=5432, dbname="testing", user="postgres", password="pass")
cur = conn.cursor()


def check_white_pixels(image, positions):
    return any((image[y, x] > 250).all() for x, y in positions)


def crop(image, box):
    x1, x2, y1, y2 = box
    return image[y1:y2, x1:x2]


def get_team(i):
    return "blue" if i < 5 else "red"


def get_player_data(image, current_time, player_acc, current_layout: CropPositions.PerkLayout, hero_templates, stat_templates, iteration, final):
    hero_name, hero_score = MatchTemplates.get_hero_name(img_crop=crop(image=image, box=current_layout.hero_crop[iteration]), templates=hero_templates)

    if hero_score < 0.5:
        hero_name = player_acc.get_current_hero()

    ult_charged = False
    if image[current_layout.ult_check[iteration][1],current_layout.ult_check[iteration][0]] > 250:
        ult_charged = True

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


    numbers = {
        "eliminations": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.eliminations_crop[iteration]), templates=stat_templates),
        "assists": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.assists_crop[iteration]), templates=stat_templates),
        "deaths": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.deaths_crop[iteration]), templates=stat_templates),
        "damage": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.damage_crop[iteration]), templates=stat_templates),
        "healing": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.healing_crop[iteration]), templates=stat_templates),
        "mitigated": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.mitigated_crop[iteration]), templates=stat_templates),
    }

    snap = HeroAccumulator.Snapshot(
            game_time=current_time,
            hero=hero_name,
            **numbers
    )

    if hero_name != player_acc.get_current_hero() or final == True:
        player_acc.ingest(snap=snap)

    return [hero_name, player_acc.get_player_id(), ult_charged, minor_perk, major_perk, *numbers.values()]


def insert_hero_stats(conn, map_id, match_id, player_id, opp_id, hero_stats):
    with conn.cursor() as cur:
        for hero, stats in hero_stats.items():
            cur.execute("""
                SELECT hero_id from heroes WHERE LOWER(REPLACE(REPLACE(REPLACE(hero_name, '.', ''), ':', ''), ' ', '_')) = %s;
            """, (hero, ))
            hero_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO player_hero_map_stats (
                    player_id, match_id, map_played_id, opponent_team_id, hero_id, 
                    seconds_played, eliminations, assists, deaths, damage, healing, mitigated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """#ON CONFLICT (map_played_id, player_id, hero_id)
                #DO UPDATE SET
                    #seconds_played = EXCLUDED.seconds_played,
                    #eliminations = EXCLUDED.eliminations,
                    #assists = EXCLUDED.assists,
                    #deaths = EXCLUDED.deaths,
                    #damage = EXCLUDED.damage,
                    #healing = EXCLUDED.healing,
                    #mitigated = EXCLUDED.mitigated;
            , (
                player_id, match_id, map_id, opp_id, hero_id, stats["seconds"],
                stats["eliminations"], stats["assists"], stats["deaths"],
                stats["damage"], stats["healing"], stats["mitigated"]
            ))
    conn.commit()


def get_replays():
    cur.execute("""
    WITH losing_team AS (
        SELECT
            mm.match_id,
            mm.match_map_id,
            mm.replay_code,
            mm.team_a_score,
            mm.team_b_score,
            mm.winner_id,
            CASE
                WHEN m.team_a_id = mm.winner_id THEN m.team_b_id
                ELSE m.team_a_id
            END AS loser_id
        FROM match_maps mm
        JOIN matches m
            ON m.match_id = mm.match_id
    )
    SELECT
        lt.match_id,
        lt.match_map_id,
        lt.replay_code,
    
        CASE
            WHEN lt.team_a_score > lt.team_b_score THEN lt.winner_id
            ELSE lt.loser_id
        END AS team_a_id,
    
        CASE
            WHEN lt.team_b_score > lt.team_a_score THEN lt.winner_id
            ELSE lt.loser_id
        END AS team_b_id
    
    FROM losing_team lt
    WHERE NOT EXISTS (
        SELECT 1
        FROM player_hero_map_stats phms
        WHERE phms.map_played_id = lt.match_map_id
    )
    ORDER BY lt.match_map_id;
    """)
    all_replays = cur.fetchall()
    return all_replays


role_templates = LoadTemplates.load_role_templates()
raw_hero_templates = LoadTemplates.load_hero_portrait_templates()
hero_templates = {"blue": {}, "red": {}}
for folder, templates in raw_hero_templates.items():
    parts = folder.split(" ")
    team, role = [part.lower().strip() for part in parts]
    hero_templates[team][role] = templates
minor_perk_templates = LoadTemplates.load_minor_perk_templates()
major_perk_templates = LoadTemplates.load_major_perk_templates()
game_time_templates = LoadTemplates.load_game_time_templates()
stats_templates = LoadTemplates.load_stat_templates()


replays = get_replays()
time.sleep(5)
temp = False
for replay in replays:
    all_data = []
    previous_time = 0
    previous_frame = None
    previous_layout = CropPositions.layouts["none"]
    player_accs = [HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator()]
    if temp:
        pyautogui.moveTo(1750, 315)
        pyautogui.leftClick()
        time.sleep(1)
        pyautogui.write(replay[2])
        pyautogui.moveTo(1040, 635)
        pyautogui.leftClick()
        time.sleep(3)
        replay_check = np.array(pyautogui.screenshot())
        if replay_check[460, 1200][0] > 100:
            pyautogui.moveTo(960, 615)
            pyautogui.leftClick()
            continue
        pyautogui.moveTo(1025, 624)
        pyautogui.leftClick()
    temp = True
    time.sleep(15)
    pyautogui.keyDown('tab')
    time.sleep(5)
    game_running = True
    camera.start()
    time.sleep(1)
    while game_running:

        frame = camera.get_latest_frame()

        if previous_frame is None:
            screenshot = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
            for i in range(10):
                team_id = replay[3] if i < 5 else replay[4]
                name_img = crop(image=screenshot, box=CropPositions.layouts["none"].name_crop[i])
                player_id, player_name = ReadText.read_name(img_crop=name_img, team_id=replay[3], reader=reader, cur=cur)
                player_accs[i].set_player_id(player_id)


        if (frame[40, 450] < 20).all():
            game_running = False
            for i, acc in enumerate(player_accs):
                team = get_team(i=i)
                role = acc.get_role()

                get_player_data(image = previous_frame, current_time=previous_time, player_acc=acc, current_layout=previous_layout,
                                hero_templates=hero_templates[team][role], stat_templates=stats_templates, iteration=i, final=True)

            for i, acc in enumerate(player_accs):
                opp_id = replay[4] if i < 5 else replay[3]
                insert_hero_stats(conn=conn, map_id=replay[1], match_id=replay[0], player_id=acc.get_player_id(),
                                  opp_id=opp_id, hero_stats=acc.finalize())

        screenshot = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
        match_time = MatchTemplates.get_game_time(img_crop=screenshot, previous_time=previous_time, templates=game_time_templates)
        if match_time == "":
            continue


        if check_white_pixels(frame, CropPositions.major_perk_positions):
            layout = CropPositions.layouts["major"]
        elif check_white_pixels(frame, CropPositions.minor_perk_positions):
            layout = CropPositions.layouts["minor"]
        else:
            layout = CropPositions.layouts["none"]

        temp_data = [match_time]
        for i, acc in enumerate(player_accs):
            team = get_team(i=i)
            if acc.get_role() is None:
                role, score = MatchTemplates.get_role(crop(image=screenshot, box=layout.role_check[i]),
                                                      templates=role_templates[team])
                if score > 0.5:
                    acc.set_role(role)

            role = acc.get_role()
            stats = get_player_data(image=screenshot, current_time=match_time, player_acc=acc, current_layout=layout,
                                    hero_templates=hero_templates[team][role], stat_templates=stats_templates,
                                    iteration=i, final=False)
            temp_data.append(stats)
            all_data.append([match_time] + stats)

        previous_frame = screenshot
        previous_time = match_time
        previous_layout = layout
        print(temp_data)


    pyautogui.keyUp('tab')
    camera.stop()


    with open(f'output_{replay[2]}.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(
            ['time', 'hero', 'player_id', 'ult_charged', 'minor_perk', 'major_perk', 'eliminations', 'assists', 'deaths', 'damage', 'healing', 'mitigated'])
        writer.writerows(all_data)

    pyautogui.press("esc")