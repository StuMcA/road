from pathlib import Path
from typing import Any, Optional

from .metrics import RoadQualityMetrics
from .model_factory import ModelFactory
from .preprocessor import ImagePreprocessor


class RoadQualityService:
    """Main service for road quality assessment using deep learning"""

    def __init__(self, model_path: Optional[str] = None):
        self.model = ModelFactory.create_model(model_path)
        self.preprocessor = ImagePreprocessor()
        self._initialize()

    def _initialize(self):
        """Initialize the service components"""
        success = self.model.load_model()
        if not success:
            raise RuntimeError("Model failed to load")

    def assess_road_quality(self, image_path: str) -> Optional[RoadQualityMetrics]:
        """
        Assess road quality from a streetview image

        Args:
            image_path: Path to the input image

        Returns:
            RoadQualityMetrics object with assessment results, or None if failed
        """
        try:
            # Validate input
            if not Path(image_path).exists():
                raise FileNotFoundError(f"Image not found: {image_path}")

            # Preprocess image
            processed_image = self.preprocessor.load_and_preprocess(image_path)
            if processed_image is None:
                return None

            # Run model inference
            predictions = self.model.predict(processed_image)

            # Convert to structured metrics
            model_info = self.model.get_model_info()
            return RoadQualityMetrics.from_model_output(predictions, model_info)

        except Exception as e:
            raise RuntimeError(f"Error assessing road quality for {image_path}: {e}") from e

    def batch_assess(self, image_paths: list[str]) -> dict[str, Optional[RoadQualityMetrics]]:
        """
        Assess road quality for multiple images

        Args:
            image_paths: List of paths to input images

        Returns:
            Dictionary mapping image paths to their assessment results
        """
        results = {}
        for image_path in image_paths:
            results[image_path] = self.assess_road_quality(image_path)

        return results

    def get_service_info(self) -> dict[str, Any]:
        """Get information about the service and its components"""
        return {
            "service_version": "1.0.0",
            "model_info": self.model.get_model_info(),
            "preprocessor_config": {"target_size": self.preprocessor.target_size},
        }
