import sys, os
from secrets_config import DB_API_KEY
import hashlib

def resource_path(relative_path: str) -> str:
    """
    relative_path should be given relative to the project root, e.g. "assets/ReadTimeModel.pth"
    """
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        base_path = sys._MEIPASS  # PyInstaller onefile temp extraction dir
    else:
        # This file (config.py) lives in src/, so go up one level to reach project root
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

def output_path(relative_path: str) -> str:
    """
    For files the app WRITES (CSVs, logs) — resolves into a writable,
    persistent user data folder, not next to the exe or into the temp bundle.
    """
    if getattr(sys, 'frozen', False):
        base_path = os.path.join(os.environ["APPDATA"], "OverwatchStatsElo")
    else:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

def hash_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

