import cv2
from functools import lru_cache
import numpy as np
import psycopg2
from typing import Optional, Tuple
import easyocr
import jellyfish
from config import API_KEY
import requests
import json


def preprocess_image(gray: np.ndarray) -> np.ndarray:
    """
    Applies filters to cropped images to normalize text contrast before passing them to EasyOCR models.
    """
    # Double the dimensions horizontally using cubic interpolation to thicken text lines
    gray = cv2.resize(gray, None, fx=2.0, fy=1.0, interpolation=cv2.INTER_CUBIC)
    # Isolate light gray text from dark background
    _, thresh = cv2.threshold(gray, 180, 255, cv2.THRESH_BINARY)
    # Apply a Gaussian blur to smooth out pixelation artifacts along character edges
    blurred = cv2.GaussianBlur(thresh, (3, 3),  0)
    # Invert the binarized image so the OCR evaluates black text on a white background
    blurred = cv2.bitwise_not(blurred)
    return blurred


def lookup_player(name: str, players: dict[int, str]) -> Optional[Tuple[int, str]]:
    """
    Queries database registries using trigram similarity metrics.
    Resolves OCR reads containing typos or misread characters to the closest valid player name on that team.
    """
    best_id = None
    best_name = None
    best_sim = 0.0

    for player_id, player_name in players.items():
        similarity = jellyfish.jaro_similarity(name.lower(), player_name.lower())

        if similarity > best_sim:
            best_sim = similarity
            best_id = player_id
            best_name = player_name

    if best_id is None:
        return None

    return best_id, best_name


def read_name(img_crop: np.ndarray, players: dict[int, str], reader: easyocr.Reader) -> Optional[Tuple[int, str]]:
    """
    Orchestrates player name extraction and identity resolution.
    """
    processed_img = preprocess_image(img_crop)
    name = reader.readtext(processed_img, allowlist='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', detail=0)[0]
    if not name:
        return None
    return lookup_player(name, players)