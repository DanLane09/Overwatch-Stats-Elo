import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import cv2
from typing import Dict, List, Any


def load_hero_portrait_templates() -> Dict[str, List[Dict[str, Any]]]:
    """
    Loads the 2D-hero icons shown on the scoreboard for both red and blue team.
    """
    all_templates = {}
    for folder in sorted(os.listdir("./Images/Scoreboard Hero Icons")):
        templates = []
        path = f"./Images/Scoreboard Hero Icons/{folder}"
        for fname in os.listdir(path):
            img = cv2.imread(os.path.join(path, fname), cv2.IMREAD_GRAYSCALE)
            role = fname.rsplit("_", 1)[0] # Isolate hero names from the filenames
            templates.append({
                "name": role,
                "img": img,
            })
        all_templates[folder] = templates
    print(f"Loaded {sum(len(template) for template in all_templates.values())} role templates")
    return all_templates


def load_stat_templates() -> List[Dict[str, Any]]:
    """
    Loads pre-processed examples of each digit (0-9) used on the scoreboard.
    """
    templates = []
    for fname in os.listdir("./Images/Stats Numbers"):
        img = cv2.imread(os.path.join("./Images/Stats Numbers", fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "number": fname.strip(".png"), # Isolates name of the digit by removing file extension
            "img": img,
        })
    print(f"Loaded {len(templates)} stat number templates")
    return templates

def load_role_templates() -> Dict[str, List[Dict[str, Any]]]:
    """
    Loads role specific emblems (Tank, Damage, Support).
    """
    all_templates = {}
    for folder in sorted(os.listdir("./Images/Scoreboard Role Icons")):
        templates = []
        path = f"./Images/Scoreboard Role Icons/{folder}"
        for fname in os.listdir(path):
            img = cv2.imread(os.path.join(path, fname), cv2.IMREAD_GRAYSCALE)
            role = fname.rsplit("_", 1)[0] # Isolate hero names from the filenames
            templates.append({
                "name": role,
                "img": img,
            })
        all_templates[folder] = templates
    print(f"Loaded {sum(len(template) for template in all_templates.values())} role templates")
    return all_templates

def load_minor_perk_templates() -> Dict[str, List[Dict[str, Any]]]:
    """
    Loads minor perk icons under each hero.
    """
    all_templates = {}
    for folder in os.listdir(f"./Images/Perks/Minor"):
        path = f"./Images/Perks/Minor/{folder}"
        templates = []
        for fname in os.listdir(path):
            img = cv2.imread(os.path.join(path, fname),  cv2.IMREAD_GRAYSCALE)
            templates.append({
                "name": fname[:-4], # Isolates name of the digit by removing file extension
                "img": img
            })
        all_templates[folder] = templates
    return all_templates

def load_major_perk_templates() -> Dict[str, List[Dict[str, Any]]]:
    """
    Loads major perk icons under each hero.
    """
    all_templates = {}
    for folder in os.listdir(f"./Images/Perks/Major"):
        path = f"./Images/Perks/Major/{folder}"
        templates = []
        for fname in os.listdir(path):
            img = cv2.imread(os.path.join(path, fname),  cv2.IMREAD_GRAYSCALE)
            templates.append({
                "name": fname[:-4], # Isolates name of the digit by removing file extension
                "img": img
            })
        all_templates[folder] = templates
    return all_templates
