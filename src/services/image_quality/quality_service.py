import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from .failure_reasons import ImageFailureReason
from .heuristics import check_image_quality
from .quality_metrics import ImageQualityMetrics
from .segmentation import RoadSegmentation


# Add config to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from config import QualityConfig


class ImageQualityService:
    """Service for assessing image quality before road analysis"""

    def __init__(self, config: Optional[QualityConfig] = None):
        self.config = config or QualityConfig()
        self.segmentation = RoadSegmentation()
        self.version = "1.0.0"

    def evaluate(self, image_path: str) -> ImageQualityMetrics:
        """
        Evaluate image quality for road analysis suitability

        Args:
            image_path: Path to image file

        Returns:
            ImageQualityMetrics with quality assessment
        """
        try:
            # Validate input
            if not Path(image_path).exists():
                return ImageQualityMetrics.create_failed(
                    image_path, ImageFailureReason.FILE_NOT_FOUND
                )

            # Stage 1: Heuristic checks (fast pre-filter)
            heuristics_result = check_image_quality(image_path, self.config)

            # Calculate heuristic scores
            blur_score = self._calculate_blur_score(heuristics_result["blurry"])
            exposure_score = self._calculate_exposure_score(heuristics_result["poor_exposure"])
            size_score = self._calculate_size_score(heuristics_result["too_small"])

            # Early termination: Skip expensive AI if heuristics fail
            heuristic_failure_reasons = self._get_heuristic_failure_reasons(heuristics_result)

            if heuristic_failure_reasons:
                # Failed heuristics - skip expensive segmentation
                return ImageQualityMetrics(
                    image_path=image_path,
                    overall_score=min(blur_score, exposure_score, size_score),
                    is_usable=False,
                    failure_reasons=heuristic_failure_reasons,
                    blur_score=blur_score,
                    exposure_score=exposure_score,
                    size_score=size_score,
                    road_surface_percentage=0.0,  # Not computed
                    has_sufficient_road=False,
                    timestamp=datetime.now().isoformat(),
                    assessment_version=self.version,
                )

            # Stage 2: AI segmentation (expensive - only if heuristics pass)
            road_percentage, has_sufficient_road = self.segmentation.detect_road_surface(image_path)

            # Calculate overall score with road surface factor
            overall_score = self._calculate_overall_score(
                blur_score, exposure_score, size_score, road_percentage
            )

            # Only segmentation can fail at this point (heuristics already passed)
            failure_reasons = []
            if not has_sufficient_road:
                failure_reasons.append(ImageFailureReason.INSUFFICIENT_ROAD_SURFACE)

            # Determine if image is usable
            is_usable = len(failure_reasons) == 0

            return ImageQualityMetrics(
                image_path=image_path,
                overall_score=overall_score,
                is_usable=is_usable,
                failure_reasons=failure_reasons,
                blur_score=blur_score,
                exposure_score=exposure_score,
                size_score=size_score,
                road_surface_percentage=road_percentage,
                has_sufficient_road=has_sufficient_road,
                timestamp=datetime.now().isoformat(),
                assessment_version=self.version,
            )

        except Exception:
            return ImageQualityMetrics.create_failed(
                image_path, ImageFailureReason.PROCESSING_ERROR
            )

    def _calculate_blur_score(self, blur_result: tuple) -> float:
        """Calculate blur quality score (0-100)"""
        is_blurry, lap_var = blur_result
        if is_blurry:
            # Map laplacian variance to score (higher variance = less blurry)
            return min(100.0, max(0.0, lap_var))
        return 100.0

    def _calculate_exposure_score(self, exposure_result: tuple) -> float:
        """Calculate exposure quality score (0-100)"""
        is_poor, (dark_frac, bright_frac) = exposure_result
        if is_poor:
            # Penalize high dark or bright fractions
            penalty = max(dark_frac, bright_frac) * 100
            return max(0.0, 100.0 - penalty)
        return 100.0

    def _calculate_size_score(self, size_result: tuple) -> float:
        """Calculate size quality score (0-100)"""
        is_too_small, (width, height) = size_result
        if is_too_small:
            # Score based on how close to minimum requirements
            min_pixels = 400 * 300
            actual_pixels = width * height
            return min(100.0, (actual_pixels / min_pixels) * 100)
        return 100.0

    def _calculate_overall_score(
        self, blur_score: float, exposure_score: float, size_score: float, road_percentage: float
    ) -> float:
        """Calculate weighted overall quality score"""
        # Weighted combination of scores
        heuristic_score = blur_score * 0.4 + exposure_score * 0.3 + size_score * 0.3

        # Road surface bonus/penalty
        road_bonus = min(20.0, road_percentage / 5.0) if road_percentage >= 25.0 else -30.0

        overall = heuristic_score + road_bonus
        return max(0.0, min(100.0, overall))

    def _get_heuristic_failure_reasons(self, heuristics_result: dict) -> list[ImageFailureReason]:
        """Determine heuristic failure reasons (fast checks only)"""
        reasons = []

        if heuristics_result["blurry"][0]:
            reasons.append(ImageFailureReason.TOO_BLURRY)

        if heuristics_result["poor_exposure"][0]:
            dark_frac, bright_frac = heuristics_result["poor_exposure"][1]
            if dark_frac > self.config.dark_threshold:
                reasons.append(ImageFailureReason.TOO_DARK)
            if bright_frac > self.config.bright_threshold:
                reasons.append(ImageFailureReason.TOO_BRIGHT)

        if heuristics_result["too_small"][0]:
            reasons.append(ImageFailureReason.RESOLUTION_TOO_SMALL)

        return reasons
