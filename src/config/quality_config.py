"""
Configuration for image quality assessment thresholds and parameters
"""

import os
from dataclasses import dataclass
from typing import Tuple


@dataclass
class QualityConfig:
    """Configuration for image quality assessment"""
    
    # Blur detection
    blur_threshold: float = 50.0
    
    # Size requirements
    min_width: int = 400
    min_height: int = 300
    
    # Exposure thresholds
    dark_threshold: float = 0.2  # Fraction of pixels that are too dark
    bright_threshold: float = 0.8  # Fraction of pixels that are too bright
    dark_pixel_value: int = 50  # Pixel intensity considered "dark"
    bright_pixel_value: int = 205  # Pixel intensity considered "bright"
    
    # Road surface requirements
    min_road_surface_percentage: float = 25.0
    
    # Scoring weights
    blur_weight: float = 0.4
    exposure_weight: float = 0.3
    size_weight: float = 0.3
    
    # Road surface scoring
    road_bonus_threshold: float = 25.0
    max_road_bonus: float = 20.0
    insufficient_road_penalty: float = -30.0
    
    @classmethod
    def from_env(cls) -> "QualityConfig":
        """Create config from environment variables with fallbacks"""
        return cls(
            blur_threshold=float(os.getenv("BLUR_THRESHOLD", "50.0")),
            min_width=int(os.getenv("MIN_IMAGE_WIDTH", "400")),
            min_height=int(os.getenv("MIN_IMAGE_HEIGHT", "300")),
            dark_threshold=float(os.getenv("DARK_THRESHOLD", "0.2")),
            bright_threshold=float(os.getenv("BRIGHT_THRESHOLD", "0.8")),
            min_road_surface_percentage=float(os.getenv("MIN_ROAD_SURFACE", "25.0")),
        )
    
    def validate(self) -> None:
        """Validate configuration parameters"""
        if self.blur_threshold <= 0:
            raise ValueError("Blur threshold must be positive")
        if self.min_width <= 0 or self.min_height <= 0:
            raise ValueError("Image size requirements must be positive")
        if not 0 < self.dark_threshold < 1:
            raise ValueError("Dark threshold must be between 0 and 1")
        if not 0 < self.bright_threshold < 1:
            raise ValueError("Bright threshold must be between 0 and 1")
        if self.min_road_surface_percentage < 0:
            raise ValueError("Road surface percentage cannot be negative")
