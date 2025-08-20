from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .failure_reasons import ImageFailureReason


@dataclass
class ImageQualityMetrics:
    """Image quality assessment results"""

    image_path: str
    overall_score: float  # 0-100
    is_usable: bool
    failure_reasons: list[ImageFailureReason]

    # Heuristic check results
    blur_score: float
    exposure_score: float
    size_score: float

    # Segmentation results
    road_surface_percentage: float
    has_sufficient_road: bool

    # Metadata
    timestamp: str
    assessment_version: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "image_path": self.image_path,
            "overall_score": self.overall_score,
            "is_usable": self.is_usable,
            "failure_reasons": [reason.value for reason in self.failure_reasons],
            "failure_messages": ImageFailureReason.get_display_messages(self.failure_reasons),
            "heuristic_checks": {
                "blur_score": self.blur_score,
                "exposure_score": self.exposure_score,
                "size_score": self.size_score,
            },
            "segmentation_checks": {
                "road_surface_percentage": self.road_surface_percentage,
                "has_sufficient_road": self.has_sufficient_road,
            },
            "metadata": {
                "timestamp": self.timestamp,
                "assessment_version": self.assessment_version,
            },
        }

    @classmethod
    def create_failed(
        cls, image_path: str, failure_reason: ImageFailureReason
    ) -> "ImageQualityMetrics":
        """Create a failed quality assessment result"""
        return cls(
            image_path=image_path,
            overall_score=0.0,
            is_usable=False,
            failure_reasons=[failure_reason],
            blur_score=0.0,
            exposure_score=0.0,
            size_score=0.0,
            road_surface_percentage=0.0,
            has_sufficient_road=False,
            timestamp=datetime.now().isoformat(),
            assessment_version="1.0.0",
        )
