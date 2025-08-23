"""
Configuration for road quality analysis parameters
"""

import os
from dataclasses import dataclass


@dataclass
class RoadConfig:
    """Configuration for road quality analysis"""
    default_bbox = [55.930362,-3.307996,55.995002,-3.124249]

    # Model settings
    default_model_type: str = "yolo"
    model_confidence_threshold: float = 0.25

    # Image preprocessing
    target_image_size: tuple = (224, 224)

    # Quality scoring thresholds
    excellent_threshold: float = 80.0
    good_threshold: float = 60.0
    fair_threshold: float = 40.0

    # Crack severity mapping
    minor_crack_threshold: float = 0.2
    moderate_crack_threshold: float = 0.5
    severe_crack_threshold: float = 0.8

    @classmethod
    def from_env(cls) -> "RoadConfig":
        """Create config from environment variables with fallbacks"""
        return cls(
            model_confidence_threshold=float(os.getenv("ROAD_MODEL_CONFIDENCE", "0.25")),
            excellent_threshold=float(os.getenv("EXCELLENT_ROAD_THRESHOLD", "80.0")),
            good_threshold=float(os.getenv("GOOD_ROAD_THRESHOLD", "60.0")),
            fair_threshold=float(os.getenv("FAIR_ROAD_THRESHOLD", "40.0")),
        )
