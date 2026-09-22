import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
import cv2
from typing import Dict, List, Any
from config import resource_path


def load_escort_score_templates() -> List[Dict[str, Any]]:
    templates = []
    folder = resource_path("assets/Images/Score numbers/Escort")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "number": fname.strip(".png"),  # Isolates name of the digit by removing file extension
            "img": img,
        })
    print(f"Loaded {len(templates)} escort score number templates")
    return templates

def load_control_score_templates() -> List[Dict[str, Any]]:
    templates = []
    folder = resource_path("assets/Images/Score numbers/Control")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "name": fname.strip(".png"),  # Isolates name of the digit by removing file extension
            "img": img,
        })
    print(f"Loaded {len(templates)} control score templates")
    return templates

def load_flashpoint_score_templates() -> List[Dict[str, Any]]:
    templates = []
    folder = resource_path("assets/Images/Score numbers/Flashpoint")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "name": fname.strip(".png"),  # Isolates name of the digit by removing file extension
            "img": img,
        })
    print(f"Loaded {len(templates)} flashpoint score templates")
    return templates

def load_percentage_templates() -> List[Dict[str, Any]]:
    """
    Loads pre-processed examples of each digit (0-9) used on the scoreboard.
    """
    templates = []
    folder = resource_path("assets/Images/Percentage numbers")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "number": fname.strip(".png"), # Isolates name of the digit by removing file extension
            "img": img,
        })
    print(f"Loaded {len(templates)} percentage number templates")
    return templates

def load_control_point_templates() -> List[Dict[str, Any]]:
    """
    Loads pre-processed examples of each control point letter.
    """
    templates = []
    folder = resource_path("assets/Images/Point Letters/Control")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "name": fname.strip(".png"),
            "img": img,
        })
    print(f"Loaded {len(templates)} control point templates")
    return templates

def load_flashpoint_point_templates() -> List[Dict[str, Any]]:
    """
    Loads pre-processed examples of each flashpoint letter.
    """
    templates = []
    folder = resource_path("assets/Images/Point Letters/Flashpoint")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "name": fname.strip(".png"),
            "img": img,
        })
    print(f"Loaded {len(templates)} flashpoint point templates")
    return templates

def load_push_decimals() -> List[Dict[str, Any]]:
    """
    Loads pre-processed examples of each digit (0-9) for the decimal used in the distance for push.
    """
    templates = []
    folder = resource_path("assets/Images/Push Decimal Distance")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
        templates.append({
            "number": fname.strip(".png"),  # Isolates name of the digit by removing file extension
            "img": img,
        })
    print(f"Loaded {len(templates)} push decimal number templates")
    return templates


def load_hero_portrait_templates() -> Dict[str, List[Dict[str, Any]]]:
    """
    Loads the 2D-hero icons shown on the scoreboard for both red and blue team.
    """
    all_templates = {}
    folders = resource_path("assets/Images/Scoreboard Hero Icons")
    for folder in sorted(os.listdir(folders)):
        templates = []
        path = resource_path(f"assets/Images/Scoreboard Hero Icons/{folder}")
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
    folder = resource_path("assets/Images/Stats Numbers")
    for fname in os.listdir(folder):
        img = cv2.imread(os.path.join(folder, fname), cv2.IMREAD_GRAYSCALE)
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
    folders = resource_path("assets/Images/Scoreboard Role Icons")
    for folder in sorted(os.listdir(folders)):
        templates = []
        path = resource_path(f"assets/Images/Scoreboard Role Icons/{folder}")
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
    folders = resource_path("assets/Images/Perks/Minor")
    for folder in os.listdir(folders):
        path = resource_path(f"assets/Images/Perks/Minor/{folder}")
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
    folders = resource_path("assets/Images/Perks/Major")
    for folder in os.listdir(folders):
        path = resource_path(f"assets/Images/Perks/Major/{folder}")
        templates = []
        for fname in os.listdir(path):
            img = cv2.imread(os.path.join(path, fname),  cv2.IMREAD_GRAYSCALE)
            templates.append({
                "name": fname[:-4], # Isolates name of the digit by removing file extension
                "img": img
            })
        all_templates[folder] = templates
    return all_templates
