import cv2
from functools import lru_cache


def preprocess_image(gray):
    gray = cv2.resize(gray, None, fx=2.0, fy=1.0, interpolation=cv2.INTER_CUBIC)
    _, thresh = cv2.threshold(gray, 180, 255, cv2.THRESH_BINARY)
    blurred = cv2.GaussianBlur(thresh, (3, 3),  0)
    blurred = cv2.bitwise_not(blurred)
    return blurred


@lru_cache(maxsize=256)
def lookup_player(name, team_id, cur):
    cur.execute("""
        SELECT player_id, name
        FROM players
        WHERE current_team_id = %s
        ORDER BY similarity(LOWER(name), %s) DESC
        LIMIT 1;
    """, (team_id, name))
    row = cur.fetchone()
    return row


def read_name(img_crop, team_id, reader, cur):
    processed_img = preprocess_image(img_crop)
    name = reader.readtext(processed_img, allowlist='abcdefghijklmnopqrstuvwxyz0123456789', detail=0)[0]
    print(name)
    if not name: return None

    return lookup_player(name, team_id, cur)