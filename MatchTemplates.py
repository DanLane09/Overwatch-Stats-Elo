import cv2
from skimage.metrics import structural_similarity as ssim
from dataclasses import dataclass
import numpy as np
import CropPositions

def preprocess_image(crop):
    scaled = cv2.resize(crop, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    enhanced = clahe.apply(scaled)
    _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return binary


def ssim_match(crop, templates):
    best_score = -1
    best_name = None

    for t in templates:
        score = ssim(crop, t["img"])

        if score > best_score:
            best_score = score
            best_name = t["name"]

    return best_name, best_score


def get_game_time(img_crop, previous_time, templates):
    mins = ""
    secs = ""
    for i in range(len(CropPositions.game_time_crop)):
        time_crop = img_crop[CropPositions.game_time_crop[i][2]:CropPositions.game_time_crop[i][3], CropPositions.game_time_crop[i][0]:CropPositions.game_time_crop[i][1]]
        number, number_probability = ssim_match(time_crop, templates)
        if number_probability > 0.5:
            if i == 0 or i == 1:
                mins += number
            else:
                secs += number
    if mins == "" or secs == "":
        return mins + secs
    else:
        if previous_time % 10 == (7 or 8) and secs[1] == '6':
            secs = secs[0] + '8'
        total_seconds = int(mins) * 60 + int(secs)
    return total_seconds


def get_hero_name(img_crop, templates):
    return ssim_match(img_crop, templates)





@dataclass
class DigitMatch:
    digit: str
    x: int
    y: int
    confidence: float
    width: int
    height: int


def match_digits(image, templates, threshold = 0.7):
    matches = []
    image = preprocess_image(image)
    for template in templates:
        number = template["number"]
        binary = template["img"]
        template_height, template_width = binary.shape[:2]
        image_height, image_width = image.shape[:2]

        if template_height > image_height or template_width > image_width:
            continue

        result = cv2.matchTemplate(image, binary, cv2.TM_CCOEFF_NORMED)
        locations = np.where(result > threshold)

        for y, x in zip(*locations):
            confidence = float(result[y, x])
            matches.append(DigitMatch(digit=number, x=int(x), y=int(y), confidence=confidence, width=template_width,
                                      height=template_height, ))

    return matches


def non_max_suppression(matches, min_distance = 20):
    sorted_matches = sorted(matches, key=lambda m: -m.confidence)

    kept = []

    for match in sorted_matches:
        overlaps = False
        for kept_match in kept:
            if abs(match.x - kept_match.x) < min_distance:
                overlaps = True
                break

        if not overlaps:
            kept.append(match)

    return kept


def read_stat(img_crop, templates, threshold = 0.7):
    matches = []
    image = preprocess_image(img_crop)
    for template in templates:
        number = template["number"]
        binary = template["img"]
        template_height, template_width = binary.shape[:2]
        image_height, image_width = image.shape[:2]

        if template_height > image_height or template_width > image_width:
            continue

        result = cv2.matchTemplate(image, binary, cv2.TM_CCOEFF_NORMED)
        locations = np.where(result > threshold)

        for y, x in zip(*locations):
            confidence = float(result[y, x])
            matches.append(DigitMatch(digit=number, x=int(x), y=int(y), confidence=confidence, width=template_width,
                                      height=template_height, ))

    min_distance = 20
    sorted_matches = sorted(matches, key=lambda m: -m.confidence)

    kept = []

    for match in sorted_matches:
        overlaps = False
        for kept_match in kept:
            if abs(match.x - kept_match.x) < min_distance:
                overlaps = True
                break

        if not overlaps:
            kept.append(match)

    sorted_matches = sorted(kept, key=lambda m: m.x)
    return "".join(m.digit for m in sorted_matches)
