import numpy as np
from typing import Dict, Any, Optional
from pathlib import Path

class RoadQualityModel:
    """Base class for road quality assessment models"""
    
    def __init__(self, model_path: Optional[str] = None):
        self.model_path = model_path
        self.model = None
        self.is_loaded = False
        
    def load_model(self) -> bool:
        """Load the trained model"""
        raise NotImplementedError("Subclasses must implement load_model")
    
    def predict(self, image_batch: np.ndarray) -> Dict[str, Any]:
        """Generate road quality predictions from preprocessed image batch"""
        raise NotImplementedError("Subclasses must implement predict")
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model"""
        raise NotImplementedError("Subclasses must implement get_model_info")
    