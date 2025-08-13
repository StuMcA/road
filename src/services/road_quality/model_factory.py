"""
Factory for creating road quality models
"""

from typing import Optional
from .yolo_model import YOLOv8RoadModel

class ModelFactory:
    """Factory for creating road quality models"""
    
    @classmethod
    def create_model(cls, model_path: Optional[str] = None):
        """
        Create a YOLOv8 road quality model
        
        Args:
            model_path: Path to pre-trained model file (optional)
        
        Returns:
            YOLOv8RoadModel instance
        """
        return YOLOv8RoadModel(model_path)
    