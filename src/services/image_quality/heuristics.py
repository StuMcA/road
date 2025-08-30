import cv2
import sys
from pathlib import Path

# Add config to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config import QualityConfig


def is_blurry(image, config: QualityConfig):
    """Check if image is blurry using Laplacian variance."""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    lap_var = cv2.Laplacian(gray, cv2.CV_64F).var()
    return lap_var < config.blur_threshold, lap_var


def is_exposed_poorly(image, config: QualityConfig):
    """Check if image is too dark or too bright based on histogram."""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    hist = cv2.calcHist([gray], [0], None, [256], [0, 256])
    hist = hist / hist.sum()  # normalize

    dark_frac = hist[:config.dark_pixel_value].sum()
    bright_frac = hist[config.bright_pixel_value:].sum()

    too_dark = dark_frac > config.dark_threshold
    too_bright = bright_frac > config.bright_threshold

    return too_dark or too_bright, (dark_frac, bright_frac)


def is_too_small(image, config: QualityConfig):
    """Reject images smaller than the given resolution."""
    h, w = image.shape[:2]
    return w < config.min_width or h < config.min_height, (w, h)

def check_image_quality(image_path, config: QualityConfig = None):
    """Run all Stage 1 checks and return results."""
    if config is None:
        config = QualityConfig()
        
    image = cv2.imread(image_path)
    if image is None:
        raise ValueError(f"Cannot load image: {image_path}")

    results = {}

    blur_flag, blur_score = is_blurry(image, config)
    results["blurry"] = (blur_flag, blur_score)

    exposure_flag, exposure_vals = is_exposed_poorly(image, config)
    results["poor_exposure"] = (exposure_flag, exposure_vals)

    size_flag, size_vals = is_too_small(image, config)
    results["too_small"] = (size_flag, size_vals)

    results["usable"] = not (blur_flag or exposure_flag or size_flag)

    return results
