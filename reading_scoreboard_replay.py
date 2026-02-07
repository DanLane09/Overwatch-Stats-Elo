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


def get_player_data(image, current_time, current_layout: CropPositions.PerkLayout, hero_templates, stat_templates, final):
    player_stats = []
    for i in range(10):
        ult_charged = False
        hero_name, hero_score = MatchTemplates.get_hero_name(img_crop=crop(image=image, box=current_layout.hero_crop[i]), templates=hero_templates)

        if hero_score > 0.5:
            hero = hero_name.rsplit("_", 1)[0] if hero_name else "unknown"
        else:
            hero = player_accs[i].get_current_hero()

        if check_white_pixels(image=image, positions=current_layout.ult_check[i]):
            ult_charged = True

        numbers = {
            "eliminations": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.eliminations_crop[i]), templates=stat_templates),
            "assists": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.assists_crop[i]), templates=stat_templates),
            "deaths": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.deaths_crop[i]), templates=stat_templates),
            "damage": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.damage_crop[i]), templates=stat_templates),
            "healing": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.healing_crop[i]), templates=stat_templates),
            "mitigated": MatchTemplates.read_stat(img_crop=crop(image=image, box=current_layout.mitigated_crop[i]), templates=stat_templates),
        }

        snap = HeroAccumulator.Snapshot(
                game_time=current_time,
                hero=hero,
                **numbers
        )

        if hero != player_accs[i].get_current_hero() or final == True:
            player_accs[i].ingest(snap=snap)

        player_stats.append([hero, player_accs[i].get_player_id(), ult_charged, *numbers.values()])

    return player_stats


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



hero_portrait_templates = LoadTemplates.load_hero_portrait_templates()
game_time_templates = LoadTemplates.load_game_time_templates()
stats_templates = LoadTemplates.load_stat_templates()


replays = get_replays()
time.sleep(5)
for replay in replays:
    all_data = []
    previous_time = 0
    previous_frame = []
    previous_layout = CropPositions.layouts["none"]
    player_accs = [HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
                   HeroAccumulator.HeroAccumulator()]
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
    time.sleep(15)
    pyautogui.keyDown('tab')
    time.sleep(5)
    game_running = True
    camera.start()
    time.sleep(1)
    while game_running:

        frame = camera.get_latest_frame()

        if previous_frame == []:
            screenshot = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
            for i in range(10):
                name_img = crop(image=screenshot, box=CropPositions.layouts["none"].name_crop[i])
                if i < 5:
                    player_id, player_name = ReadText.read_name(img_crop=name_img, team_id=replay[3], reader=reader,
                                                                cur=cur)
                else:
                    player_id, player_name = ReadText.read_name(img_crop=name_img, team_id=replay[4], reader=reader,
                                                                cur=cur)
                player_accs[i].set_player_id(player_id)

        if (frame[40, 450] < 20).all():
            game_running = False
            get_player_data(image=previous_frame, current_time=previous_time, current_layout=previous_layout,
                            hero_templates=hero_portrait_templates, stat_templates=stats_templates, final=True)
            for i in range(len(player_accs)):
                if i < 5:
                    insert_hero_stats(conn=conn, map_id=replay[1], match_id=replay[0], player_id=player_accs[i].player_id,
                                      opp_id=replay[4], hero_stats=player_accs[i].finalize())
                else:
                    insert_hero_stats(conn=conn, map_id=replay[1], match_id=replay[0], player_id=player_accs[i].player_id,
                                      opp_id=replay[3], hero_stats=player_accs[i].finalize())

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

        stats = get_player_data(image=screenshot, current_time=match_time, current_layout=layout,
                                hero_templates=hero_portrait_templates, stat_templates=stats_templates, final=False)

        previous_frame = screenshot
        previous_time = match_time
        previous_layout = layout
        print(match_time, stats)
        for stat in stats:
            all_data.append([match_time] + stat)

    pyautogui.keyUp('tab')
    camera.stop()


    with open(f'output_{replay[2]}.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(
            ['time', 'hero', 'player_id', 'ult_charged', 'eliminations', 'assists', 'deaths', 'damage', 'healing', 'mitigated'])
        writer.writerows(all_data)

    pyautogui.press("esc")