import cv2
from skimage.metrics import structural_similarity as ssim
from dataclasses import dataclass
import numpy as np
from typing import List, Dict, Any, Tuple, Optional


def preprocess_image(crop: np.ndarray) -> np.ndarray:
    """
    Applies filters to optimize images for matching.
    """
    scaled = cv2.resize(crop, None, fx=4.0, fy=4.0, interpolation=cv2.INTER_CUBIC) # Upscale image
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)) # Balances uneven lighting
    enhanced = clahe.apply(scaled)
    _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU) # Turns image black and white
    return binary

def crop(image: np.ndarray, box: list[int]) -> np.ndarray:
    """
    Extracts a region from an image using [x_start, x_end, y_start, y_end] box limits.
    """
    x1, x2, y1, y2 = box
    return image[y1:y2, x1:x2]

def ssim_match(crop: np.ndarray, templates: List[Dict[str, Any]]) -> Tuple[Optional[str], float]:
    """
    Compares a cropped region against icon templates using the Structural Similarity Index (SSIM).
    """
    best_score = -1
    best_name = None

    for t in templates:
        score = ssim(crop, t["img"])
        if score > best_score:
            best_score = score
            best_name = t["name"]

    return best_name, best_score

# Helper abstractions mapping specialized calls to the standard SSIM matching pipeline
def get_hero_name(img_crop: np.ndarray, templates: List[Dict[str, Any]]) -> Tuple[Optional[str], float]:
    return ssim_match(img_crop, templates)

def get_role(image: np.ndarray, templates: List[Dict[str, Any]]) -> Tuple[Optional[str], float] :
    return ssim_match(image, templates)

def get_perk(img_crop: np.ndarray, templates: List[Dict[str, Any]]) -> Tuple[Optional[str], float]:
    return ssim_match(img_crop, templates)

def get_escort_score(img: np.ndarray, crop_positions: Dict[str, List[int]], templates: List[Dict[str, Any]]) -> List[Any]:
    scores = []
    for value in crop_positions.values():
        cropped_img = crop(img, value)
        _, thresh = cv2.threshold(cropped_img, 180, 255, cv2.THRESH_BINARY)
        digit = read_number(image=thresh, templates=templates)
        if digit != "":
            scores.append(int(digit))
        else:
            scores.append(-1)
    return scores

def get_control_score(img: np.ndarray, crop_positions: Dict[str, List[int]], templates: List[Dict[str, Any]], targets: List[List[int]]) -> List[Any]:
    scores = []
    for value, target in zip(crop_positions.values(), targets):
        cropped_img = crop(img, value)
        target_colour = np.array(target, dtype=np.uint8)
        mask = np.all(cropped_img == target_colour, axis=2)
        output = np.zeros_like(cropped_img)
        output[mask] = [255, 255, 255]
        output = cv2.cvtColor(output, cv2.COLOR_BGR2GRAY)
        digit, score = ssim_match(crop=output, templates=templates)
        if score > 0.5:
            scores.append(int(digit))
        else:
            scores.append(-1)
    return scores

def get_control_percentage(img: np.ndarray, crop_positions: Dict[str, List[int]], templates: List[Dict[str, Any]], targets: List[List[int]]) -> List[int]:
    percentages = []
    for value, target in zip(crop_positions.values(), targets):
        cropped_img = crop(img, value)
        target_colour = np.array(target, dtype=np.uint8)
        mask = np.all(cropped_img == target_colour, axis=2)
        output = np.zeros_like(cropped_img)
        output[mask] = [255, 255, 255]
        output = cv2.cvtColor(output, cv2.COLOR_BGR2GRAY)
        number = read_number(output, templates, 10)
        percentages.append(number)
    return percentages

def get_control_point(img: np.ndarray, crop_positions: List[int], templates: List[Dict[str, Any]]) -> str|None:
    cropped_img = crop(img, crop_positions)
    gray_img = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2GRAY)
    img = preprocess_image(gray_img)
    point, score = ssim_match(img, templates)
    if score > 0.5:
        return point
    else:
        return None


def read_stat(img_crop: np.ndarray, templates: List[Dict[str, Any]]) -> str:
    image = preprocess_image(img_crop)
    return read_number(image=image, templates=templates)

@dataclass
class DigitMatch:
    """
    Stores information about a matched number template on the screen layout.
    """
    digit: str
    x: int
    y: int
    confidence: float
    width: int
    height: int

def read_number(
    image: np.ndarray,
    templates: List[Dict[str, Any]],
    threshold: float = 0.5,
    iou_threshold: float = 0.3,
) -> str:
    """
    Detects digits in an image using template matching and Non-Maximum Suppression.
    """
    matches: list[DigitMatch] = []

    image_height, image_width = image.shape[:2]

    # Find every template match
    for template in templates:
        digit = template["number"]
        template_img = template["img"]
        height, width = template_img.shape[:2]

        if height > image_height or width > image_width:
            continue

        result = cv2.matchTemplate(image, template_img, cv2.TM_CCOEFF_NORMED)

        ys, xs = np.where(result >= threshold)

        for x, y in zip(xs, ys):
            matches.append(
                DigitMatch(
                    digit=digit,
                    x=int(x),
                    y=int(y),
                    confidence=float(result[y, x]),
                    width=width,
                    height=height,
                )
            )

    matches = non_maximum_suppression(matches, iou_threshold)

    # Used for debugging: outputs every single match, where and confidence score
    """for match in sorted(matches, key=lambda m: m.x):
        print(
            f"{match.digit}  "
            f"x={match.x:3d}  "
            f"conf={match.confidence:.3f}"
        )"""

    return "".join(
        match.digit
        for match in sorted(matches, key=lambda m: m.x)
    )

def non_maximum_suppression(
    matches: List[DigitMatch],
    iou_threshold: float,
) -> List[DigitMatch]:
    """
    Removes duplicate template matches using IoU-based Non-Maximum Suppression.
    """
    kept: list[DigitMatch] = []

    for match in sorted(matches, key=lambda m: m.confidence, reverse=True):
        if all(iou(match, other) < iou_threshold for other in kept):
            kept.append(match)

    return kept

def iou(a: DigitMatch, b: DigitMatch) -> float:
    """
    Computes the Intersection over Union (IoU) of two digit detections.
    """
    left = max(a.x, b.x)
    top = max(a.y, b.y)
    right = min(a.x + a.width, b.x + b.width)
    bottom = min(a.y + a.height, b.y + b.height)

    if right <= left or bottom <= top:
        return 0.0

    intersection = (right - left) * (bottom - top)

    area_a = a.width * a.height
    area_b = b.width * b.height

    return intersection / (area_a + area_b - intersection)
