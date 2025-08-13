import cv2
import numpy as np
from typing import Tuple, Optional
from pathlib import Path

class ImagePreprocessor:
    """Preprocesses images for road quality assessment model"""
    
    def __init__(self, target_size: Tuple[int, int] = (224, 224)):
        self.target_size = target_size
    
    def load_and_preprocess(self, image_path: str) -> Optional[np.ndarray]:
        """Load image and preprocess for model inference"""
        try:
            # Load image
            image = cv2.imread(image_path)
            if image is None:
                raise ValueError(f"Could not load image: {image_path}")
            
            # Convert BGR to RGB
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            
            # Preprocess for model
            processed = self._preprocess_for_model(image)
            return processed
            
        except Exception:
            return None
    
    def _preprocess_for_model(self, image: np.ndarray) -> np.ndarray:
        """Apply model-specific preprocessing"""
        # Resize to target size
        resized = cv2.resize(image, self.target_size)
        
        # Normalize to [0, 1]
        normalized = resized.astype(np.float32) / 255.0
        
        # Add batch dimension
        batch = np.expand_dims(normalized, axis=0)
        
        return batch
    
    def extract_road_region(self, image: np.ndarray) -> np.ndarray:
        """Extract the road region from the image (basic implementation)"""
        # Simple approach: assume road is in bottom half of image
        height = image.shape[0]
        road_region = image[height//2:, :]
        return road_region
    
    def enhance_for_analysis(self, image: np.ndarray) -> np.ndarray:
        """Enhance image contrast and clarity for analysis"""
        # Convert to LAB color space
        lab = cv2.cvtColor(image, cv2.COLOR_RGB2LAB)
        l, a, b = cv2.split(lab)
        
        # Apply CLAHE to L channel
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        l_enhanced = clahe.apply(l)
        
        # Merge channels and convert back
        enhanced_lab = cv2.merge([l_enhanced, a, b])
        enhanced = cv2.cvtColor(enhanced_lab, cv2.COLOR_LAB2RGB)
        
        return enhanced
    