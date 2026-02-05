import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import cv2 as cv
import easyocr
from skimage.metrics import structural_similarity as ssim
import psycopg2
import bettercam
import HeroAccumulator
from dataclasses import dataclass
from functools import lru_cache
import pyautogui
pyautogui.FAILSAFE = True
import time

@dataclass(frozen=True)
class PerkLayout:
    hero_crop: list
    name_crop: list
    eliminations_crop: list
    assists_crop: list
    deaths_crop: list
    damage_crop: list
    healing_crop: list
    mitigated_crop: list


player_accs = [HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(),
               HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator(), HeroAccumulator.HeroAccumulator()]


camera = bettercam.create(output_color="RGB")
reader = easyocr.Reader(["en"], gpu=True)

GAME_TIME_TEMPLATES = []
HERO_PORTRAIT_TEMPLATES = []
orb = cv.ORB_create(nfeatures=500)

conn = psycopg2.connect(host="localhost", port=5432, dbname="testing", user="postgres", password="pass")
cur = conn.cursor()

# The [x, y] position to check for a white (perks) or black (end of game) pixel
minor_perk_positions = [[862, 215], [862, 277], [862, 339], [862, 401], [862, 463],
                        [862, 608], [862, 670], [862, 732], [862, 794], [862, 856]]
major_perk_positions = [[831, 215], [831, 277], [831, 339], [831, 401], [831, 463],
                        [831, 608], [831, 670], [831, 732], [831, 794], [831, 856]]
game_end_check = [[350, 200], [450, 40], [1600, 750]]


# The positions to crop the image in the format: [x_start, x_end, y_start, y_end]
game_time_crop = [[1815, 1832, 58, 84], [1829, 1846, 58, 84], [1849, 1866, 58, 84], [1863, 1880, 58, 84]]

layouts = {
    "none": PerkLayout(
        hero_crop = [[544, 604, 207, 267], [544, 604, 269, 329], [544, 604, 331, 391], [544, 604, 393, 453], [544, 604, 455, 515],
                     [544, 604, 600, 660], [544, 604, 662, 722], [544, 604, 724, 784], [544, 604, 786, 846], [544, 604, 848, 908]],
        name_crop = [[655, 856, 210, 261], [655, 856, 272, 323], [655, 856, 334, 385], [655, 856, 396, 447], [655, 856, 458, 509],
                     [655, 856, 607, 658], [655, 856, 669, 710], [655, 856, 721, 772], [655, 856, 783, 834], [655, 856, 845, 896]],
        eliminations_crop = [[890, 943, 225, 250], [890, 943, 288, 313], [890, 943, 350, 375], [890, 943, 410, 435], [890, 943, 472, 497],
                             [890, 943, 620, 645], [890, 943, 680, 705], [890, 943, 742, 767], [890, 943, 805, 830], [890, 943, 865, 890]],
        assists_crop = [[946, 999, 225, 250], [946, 999, 288, 313], [946, 999, 350, 375], [946, 999, 410, 435], [946, 999, 472, 497],
                        [946, 999, 620, 645], [946, 999, 680, 705], [946, 999, 742, 767], [946, 999, 805, 830], [946, 999, 865, 890]],
        deaths_crop = [[1000, 1053, 225, 250], [1000, 1053, 288, 313], [1000, 1053, 350, 375], [1000, 1053, 410, 435], [1000, 1053, 472, 497],
                       [1000, 1053, 620, 645], [1000, 1053, 680, 705], [1000, 1053, 742, 767], [1000, 1053, 805, 830], [1000, 1053, 865, 890]],
        damage_crop = [[1055, 1163, 225, 250], [1055, 1163, 288, 313], [1055, 1163, 350, 375], [1055, 1163, 410, 435], [1055, 1163, 472, 497],
                       [1055, 1163, 620, 645], [1055, 1163, 680, 705], [1055, 1163, 742, 767], [1055, 1163, 805, 830], [1055, 1163, 865, 890]],
        healing_crop = [[1164, 1264, 225, 250], [1164, 1264, 288, 313], [1164, 1264, 350, 375], [1164, 1264, 410, 435], [1164, 1264, 472, 497],
                        [1164, 1264, 620, 645], [1164, 1264, 680, 705], [1164, 1264, 742, 767], [1164, 1264, 805, 830], [1164, 1264, 865, 890]],
        mitigated_crop = [[1265, 1363, 225, 250], [1265, 1363, 288, 313], [1265, 1363, 350, 375], [1265, 1363, 410, 435], [1265, 1363, 472, 497],
                          [1265, 1363, 620, 645], [1265, 1363, 680, 705], [1265, 1363, 742, 767], [1265, 1363, 805, 830], [1265, 1363, 865, 890]],
    ),
    "minor": PerkLayout(
        hero_crop = [[536, 596, 207, 267], [536, 596, 269, 329], [536, 596, 331, 391], [536, 596, 393, 453], [536, 596, 455, 515],
                     [536, 596, 600, 660], [536, 596, 662, 722], [536, 596, 724, 784], [536, 596, 786, 846], [536, 596, 848, 908]],
        name_crop = [[650, 831, 210, 261], [650, 831, 272, 323], [650, 831, 334, 385], [650, 831, 396, 447], [650, 831, 458, 509],
                     [650, 831, 607, 658], [650, 831, 669, 710], [650, 831, 731, 782], [650, 831, 783, 834], [650, 831, 845, 896]],
        eliminations_crop = [[900, 950, 225, 250], [900, 950, 288, 313], [900, 950, 350, 375], [900, 950, 410, 435], [900, 950, 472, 497],
                             [900, 950, 620, 645], [900, 950, 680, 705], [900, 950, 742, 767], [900, 950, 805, 830], [900, 950, 865, 890]],
        assists_crop = [[952, 1006, 225, 250], [952, 1006, 288, 313], [952, 1006, 350, 375], [952, 1006, 410, 435], [952, 1006, 472, 497],
                        [952, 1006, 620, 645], [952, 1006, 680, 705], [952, 1006, 742, 767], [952, 1006, 805, 830], [952, 1006, 865, 890]],
        deaths_crop = [[1008, 1060, 225, 250], [1008, 1060, 288, 313], [1008, 1060, 350, 375], [1008, 1060, 410, 435], [1008, 1060, 472, 497],
                       [1008, 1060, 620, 645], [1008, 1060, 680, 705], [1008, 1060, 742, 767], [1008, 1060, 805, 830], [1008, 1060, 865, 890]],
        damage_crop = [[1062, 1174, 225, 250], [1062, 1174, 288, 313], [1062, 1174, 350, 375], [1062, 1174, 410, 435], [1062, 1174, 472, 497],
                       [1062, 1174, 620, 645], [1062, 1174, 680, 705], [1062, 1174, 742, 767], [1062, 1174, 805, 830], [1062, 1174, 865, 890]],
        healing_crop = [[1175, 1269, 225, 250], [1175, 1269, 288, 313], [1175, 1269, 350, 375], [1175, 1269, 410, 435], [1175, 1269, 472, 497],
                        [1175, 1269, 620, 645], [1175, 1269, 680, 705], [1175, 1269, 742, 767], [1175, 1269, 805, 830], [1175, 1269, 865, 890]],
        mitigated_crop = [[1270, 1370, 225, 250], [1270, 1370, 288, 313], [1270, 1370, 350, 375], [1270, 1370, 410, 435], [1270, 1370, 472, 497],
                          [1270, 1370, 620, 645], [1270, 1370, 680, 705], [1270, 1370, 742, 767], [1270, 1370, 805, 830], [1270, 1370, 865, 890]],
    ),
    "major": PerkLayout(
        hero_crop = [[505, 565, 207, 267], [505, 565, 269, 329], [505, 565, 331, 391], [505, 565, 393, 453], [505, 565, 455, 515],
                     [505, 565, 600, 660], [505, 565, 662, 722], [505, 565, 724, 784], [505, 565, 786, 846], [505, 565, 848, 908]],
        name_crop = [[615, 801, 210, 261], [615, 801, 272, 323], [615, 801, 334, 385], [615, 801, 396, 447], [615, 801, 458, 509],
                     [615, 801, 607, 658], [615, 801, 669, 710], [615, 801, 721, 772], [615, 801, 783, 834], [615, 801, 845, 896]],
        eliminations_crop = [[930, 982, 225, 250], [930, 982, 288, 313], [930, 982, 350, 375], [930, 982, 410, 435], [930, 982, 472, 497],
                             [930, 982, 620, 645], [930, 982, 680, 705], [930, 982, 742, 767], [930, 982, 805, 830], [930, 982, 865, 890]],
        assists_crop = [[984, 1036, 225, 250], [984, 1036, 288, 313], [984, 1036, 350, 375], [984, 1036, 410, 435], [984, 1036, 472, 497],
                        [984, 1036, 620, 645], [984, 1036, 680, 705], [984, 1036, 742, 767], [984, 1036, 805, 830], [984, 1036, 865, 890]],
        deaths_crop = [[1040, 1090, 225, 250], [1040, 1090, 288, 313], [1040, 1090, 350, 375], [1040, 1090, 410, 435], [1040, 1090, 472, 497],
                       [1040, 1090, 620, 645], [1040, 1090, 680, 705], [1040, 1090, 742, 767], [1040, 1090, 805, 830], [1040, 1090, 865, 890]],
        damage_crop = [[1100, 1200, 225, 250], [1100, 1200, 288, 313], [1100, 1200, 350, 375], [1100, 1200, 410, 435], [1100, 1200, 472, 497],
                       [1100, 1200, 620, 645], [1100, 1200, 680, 705], [1100, 1200, 742, 767], [1100, 1200, 805, 830], [1100, 1200, 865, 890]],
        healing_crop = [[1205, 1300, 225, 250], [1205, 1300, 288, 313], [1205, 1300, 350, 375], [1205, 1300, 410, 435], [1205, 1300, 472, 497],
                        [1205, 1300, 620, 645], [1205, 1300, 680, 705], [1205, 1300, 742, 767], [1205, 1300, 805, 830], [1205, 1300, 865, 890]],
        mitigated_crop = [[1305, 1400, 225, 250], [1305, 1400, 288, 313], [1305, 1400, 350, 375], [1305, 1400, 410, 435], [1305, 1400, 472, 497],
                          [1305, 1400, 620, 645], [1305, 1400, 680, 705], [1305, 1400, 742, 767], [1305, 1400, 805, 830], [1305, 1400, 865, 890]],
    )
}


def crop(image, box):
    x1, x2, y1, y2 = box
    return image[y1:y2, x1:x2]

def enhance_image_name(gray):
    gray = cv.resize(gray, None, fx=1.0, fy=1.0, interpolation=cv.INTER_CUBIC)
    _, thresh = cv.threshold(gray, 180, 255, cv.THRESH_BINARY)
    blurred = cv.GaussianBlur(thresh, (3, 3),  0)
    blurred = cv.bitwise_not(blurred)
    return blurred

def enhance_image(gray):
    gray = cv.resize(gray, None, fx=3.0, fy=3.0, interpolation=cv.INTER_CUBIC)
    _, thresh = cv.threshold(gray, 180, 255, cv.THRESH_BINARY)
    blurred = cv.GaussianBlur(thresh, (7, 7),  0)
    blurred = cv.bitwise_not(blurred)
    return blurred

def backup_enhance_image(gray):
    gray = cv.resize(gray, None, fx=4.0, fy=4.0, interpolation=cv.INTER_CUBIC)
    _, thresh = cv.threshold(gray, 180, 255, cv.THRESH_BINARY)
    blurred = cv.GaussianBlur(thresh, (7, 7), 0)
    blurred = cv.bitwise_not(blurred)
    return blurred


def read_number(img_crop):
    processed_img = enhance_image(img_crop)
    results = reader.readtext(processed_img, allowlist='0123456789,', detail=0)
    if not results:
        results = reader.readtext(backup_enhance_image(img_crop), allowlist='0123456789,', detail=0)
        if results:
            temp = "".join(results)
            return temp.replace(",", "")
        return "0"
    temp = "".join(results)
    return temp.replace(",", "")


@lru_cache(maxsize=256)
def lookup_player(name, team_id):
    cur.execute("""
        SELECT player_id
        FROM players
        WHERE current_team_id = %s
        ORDER BY similarity(LOWER(name), %s) DESC
        LIMIT 1;
    """, (team_id, name))

    row = cur.fetchone()
    return row[0] if row else None


def read_name(img_crop, team_id):
    processed_img = enhance_image_name(img_crop)
    name = reader.readtext(processed_img, allowlist='abcdefghijklmnopqrstuvwxyz0123456789', detail=0)[0]

    if not name: return None

    return lookup_player(name, team_id)


def ssim_match(crop, templates):
    best_score = -1
    best_name = None

    for t in templates:
        score = ssim(crop, t["img"])

        if score > best_score:
            best_score = score
            best_name = t["name"]

    return best_name, best_score


def get_game_time(ss, previous_time):
    mins = ""
    secs = ""
    for i in range(len(game_time_crop)):
        time_crop = ss[game_time_crop[i][2]:game_time_crop[i][3], game_time_crop[i][0]:game_time_crop[i][1]]
        cv.imshow("f", time_crop)
        cv.waitKey(0)
        number, number_probability = ssim_match(time_crop, GAME_TIME_TEMPLATES)
        if number_probability > 0.5:
            if i == 0 or i == 1:
                mins += number
            else:
                secs += number
    if mins == "" or secs == "":
        total_seconds = previous_time
    else:
        total_seconds = int(mins) * 60 + int(secs)
    return total_seconds


def get_player_data(ss, time, layout: PerkLayout, replay, final):
    player_stats = []
    for i in range(10):
        hero_img = crop(ss, layout.hero_crop[i])
        hero_name, hero_score = ssim_match(hero_img, HERO_PORTRAIT_TEMPLATES)

        if hero_score > 0.5:
            hero = hero_name.rsplit("_", 1)[0] if hero_name else "unknown"
        else:
            hero = player_accs[i].get_current_hero()

        if hero != player_accs[i].get_current_hero() or final == True:
            name_img = crop(ss, layout.name_crop[i])



            if i < 5:
                player_id = read_name(name_img, replay[3])
            else:
                player_id = read_name(name_img, replay[4])

            numbers = {
                "eliminations": read_number(crop(ss, layout.eliminations_crop[i])),
                "assists": read_number(crop(ss, layout.assists_crop[i])),
                "deaths": read_number(crop(ss, layout.deaths_crop[i])),
                "damage": read_number(crop(ss, layout.damage_crop[i])),
                "healing": read_number(crop(ss, layout.healing_crop[i])),
                "mitigated": read_number(crop(ss, layout.mitigated_crop[i])),
            }

            snap = HeroAccumulator.Snapshot(
                game_time=time,
                hero=hero,
                **numbers
            )
            player_accs[i].ingest(snap, player_id)

            player_stats.append([hero, player_id, *numbers.values()])

    return player_stats


def insert_hero_stats(conn, map_id, match_id, player_id, team_id, opp_id, hero_stats):
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
                ON CONFLICT (map_played_id, player_id, hero_id)
                DO UPDATE SET
                    seconds_played = EXCLUDED.seconds_played,
                    eliminations = EXCLUDED.eliminations,
                    assists = EXCLUDED.assists,
                    deaths = EXCLUDED.deaths,
                    damage = EXCLUDED.damage,
                    healing = EXCLUDED.healing,
                    mitigated = EXCLUDED.mitigated;
            """, (
                player_id, match_id, map_id, opp_id, hero_id, stats["seconds"],
                stats["eliminations"], stats["assists"], stats["deaths"],
                stats["damage"], stats["healing"], stats["mitigated"]
            ))
    conn.commit()


def check_white_pixels(img, positions):
    return any((img[y, x] > 250).all() for x, y in positions)


for fname in os.listdir("./Images/Scoreboard Hero Icons"):
    img = cv.imread(os.path.join("./Images/Scoreboard Hero Icons", fname), cv.IMREAD_GRAYSCALE)
    kp, des = orb.detectAndCompute(img, None)
    HERO_PORTRAIT_TEMPLATES.append({
        "name": fname,
        "img": img,
        "kp": kp,
        "des": des
    })
print(f"Loaded {len(HERO_PORTRAIT_TEMPLATES)} hero templates")


for fname in os.listdir("./Images/Game Time Numbers"):
    img = cv.imread(os.path.join("./Images/Game Time Numbers", fname), cv.IMREAD_GRAYSCALE)
    GAME_TIME_TEMPLATES.append({
        "name": fname.strip(".png"),
        "img": img,
    })
print(f"Loaded {len(GAME_TIME_TEMPLATES)} game time templates")

# Gets data for played maps that need to be parsed
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

camera.start()
game_running = True
iteration = 4

time.sleep(10)

pyautogui.keyDown('tab')
time.sleep(1)
pyautogui.press('p')
previous_time = 0
previous_frame = []
previous_layout = layouts["none"]
while game_running:

    image = camera.get_latest_frame()

    if (image[40, 450] < 20).all():
        game_running = False
        get_player_data(previous_frame, previous_time, previous_layout, all_replays[iteration], True)
        for i in range(len(player_accs)):
            if i < 5:
                insert_hero_stats(conn, all_replays[iteration][1], all_replays[iteration][0], player_accs[i].player_id,
                                  all_replays[iteration][3], all_replays[iteration][4], player_accs[i].finalize())
            else:
                insert_hero_stats(conn, all_replays[iteration][1], all_replays[iteration][0], player_accs[i].player_id,
                                  all_replays[iteration][4], all_replays[iteration][3], player_accs[i].finalize())

    screenshot = cv.cvtColor(image, cv.COLOR_RGB2GRAY)
    match_time = get_game_time(screenshot, previous_time)


    if check_white_pixels(image, major_perk_positions):
        layout = layouts["major"]
    elif check_white_pixels(image, minor_perk_positions):
        layout = layouts["minor"]
    else:
        layout = layouts["none"]

    stats = get_player_data(screenshot, match_time, layout, all_replays[iteration], False)

    previous_frame = screenshot
    previous_time = match_time
    previous_layout = layout
    print(match_time, stats)
