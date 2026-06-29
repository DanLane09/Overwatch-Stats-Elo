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

def read_stat(img_crop: np.ndarray, templates: List[Dict[str, Any]], threshold: float = 0.6) -> str:
    """
    Scans a cropped image for numeric characters.
    Uses template matching combined with Non-Maximum Suppression (NMS) to parse and read the final string value.
    """
    matches = []
    image = preprocess_image(img_crop)
    # Run structural comparisons against templates for every digit 0-9
    for template in templates:
        number = template["number"]
        binary = template["img"]
        template_height, template_width = binary.shape[:2]
        image_height, image_width = image.shape[:2]

        # Skip comparisons if the target template size exceeds current crop dimensions (SHOULDN'T BE NEEDED)
        if template_height > image_height or template_width > image_width:
            continue

        # Execute normalized cross-correlation matching
        result = cv2.matchTemplate(image, binary, cv2.TM_CCOEFF_NORMED)
        locations = np.where(result > threshold)

        # Record all match hits that pass our threshold
        for y, x in zip(*locations):
            confidence = float(result[y, x])
            matches.append(DigitMatch(digit=number, x=int(x), y=int(y), confidence=confidence, width=template_width,
                                      height=template_height, ))

    # Sort detections by confidence score so we evaluate the most accurate hits first
    min_distance = 20 # Minimum pixel gap allowed between consecutive digit locations
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
    # Sort remaining unique digits from left-to-right to read the final number correctly
    sorted_matches = sorted(kept, key=lambda m: m.x)
    return "".join(m.digit for m in sorted_matches)
