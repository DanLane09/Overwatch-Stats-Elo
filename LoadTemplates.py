import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import cv2


def load_hero_portrait_templates():
    templates = []
    for fname in os.listdir("./Images/Scoreboard Hero Icons"):
        img = cv2.imread(os.path.join("./Images/Scoreboard Hero Icons", fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "name": fname,
            "img": img,
        })
    print(f"Loaded {len(templates)} hero templates")
    return templates


def load_game_time_templates():
    templates = []
    for fname in os.listdir("./Images/Game Time Numbers"):
        img = cv2.imread(os.path.join("./Images/Game Time Numbers", fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "name": fname.strip(".png"),
            "img": img,
        })
    print(f"Loaded {len(templates)} game time templates")
    return templates


def load_stat_templates():
    templates = []
    for fname in os.listdir("./Images/Stats Numbers"):
        img = cv2.imread(os.path.join("./Images/Stats Numbers", fname), cv2.IMREAD_GRAYSCALE)
        _, binary = cv2.threshold(img, 0 , 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        templates.append({
            "number": fname.strip(".png"),
            "img": binary,
        })
    print(f"Loaded {len(templates)} stat number templates")
    return templates