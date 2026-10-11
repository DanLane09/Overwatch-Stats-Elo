import json
import os

BASE_RESOLUTION = (1920, 1080)   # the resolution your current (x, y) values were measured at

RESOLUTIONS_16_9 = ["1920x1080", "2048x1152", "2560x1440", "2880x1620", "3200x1800", "3840x2160", "5120x2880", "7680x4320",]
RESOLUTIONS_16_10 = ["1920x1200", "2560x1600", "2880x1800", "3072x1920", "3200x2000", "3840x2400",]
RESOLUTIONS = RESOLUTIONS_16_9 + RESOLUTIONS_16_10

DEFAULTS = {"resolution": "1920x1080"}

SETTINGS_DIR = os.path.join(os.environ["LOCALAPPDATA"], "OverwatchStatsElo")
SETTINGS_PATH = os.path.join(SETTINGS_DIR, "settings.json")

_settings = None

def _load():
    data = dict(DEFAULTS)
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            data.update(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    if data["resolution"] not in RESOLUTIONS:
        data["resolution"] = DEFAULTS["resolution"]
    return data

def _save():
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    tmp = SETTINGS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_settings, f, indent=2)
    os.replace(tmp, SETTINGS_PATH)   # atomic, so a crash can't leave a half-written file

def get(key):
    global _settings
    if _settings is None:
        _settings = _load()
    return _settings[key]

def set(key, value):
    global _settings
    if _settings is None:
        _settings = _load()
    _settings[key] = value
    _save()

def get_resolution():
    res = get("resolution")
    w, h = map(int, res.split("x"))
    if res in RESOLUTIONS_16_9:
        return w, h, "16:9"
    return w, h, "16:10"

def scale_xy(x, y, import_botton=False):
    """Convert a coordinate measured at BASE_RESOLUTION to the selected resolution."""
    base_resolution = [1920, 1080]
    w, h, aspect_ratio = get_resolution()
    if import_botton and (aspect_ratio == "16:10"):
        base_resolution = [1920, 1200]
    return round(x * w / base_resolution[0]), round(y * h / base_resolution[1])