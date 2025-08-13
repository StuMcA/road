"""
YOLOv8-based road quality model implementation
"""

import numpy as np
from typing import Dict, Any, Optional
from pathlib import Path
from .model import RoadQualityModel

class YOLOv8RoadModel(RoadQualityModel):
    """Road quality model using YOLOv8 for defect detection"""
    
    def __init__(self, model_path: Optional[str] = None):
        super().__init__(model_path)
        
        # Class mappings for road defects
        self.class_names = {
            0: 'crack',
            1: 'pothole', 
            2: 'debris',
            3: 'lane_marking'
        }
    
    def load_model(self) -> bool:
        """Load YOLOv8 model"""
        try:
            # pip install ultralytics
            from ultralytics import YOLO
            
            if self.model_path and Path(self.model_path).exists():
                # Load custom trained model
                self.model = YOLO(self.model_path)
            else:
                # Start with base model and fine-tune
                self.model = YOLO('yolov8n.pt')  # nano version for speed
            
            self.is_loaded = True
            return True
        except ImportError:
            print("ultralytics not installed. Run: pip install ultralytics")
            return False
        except Exception as e:
            print(f"Failed to load YOLOv8 model: {e}")
            return False
    
    def predict(self, image_batch: np.ndarray) -> Dict[str, Any]:
        """Run inference on image batch"""
        if not self.is_loaded:
            raise RuntimeError("Model not loaded")
        
        # YOLOv8 expects single images, not batches
        image = image_batch[0] if len(image_batch.shape) == 4 else image_batch
        
        # Convert from normalized [0,1] back to [0,255]
        if image.max() <= 1.0:
            image = (image * 255).astype(np.uint8)
        
        # Run detection
        results = self.model(image)
        
        # Parse results
        predictions = self._parse_yolo_results(results[0])
        return predictions
    
    def _parse_yolo_results(self, result) -> Dict[str, Any]:
        """Convert YOLO results to our metrics format"""
        boxes = result.boxes
        
        crack_detections = []
        pothole_detections = []
        debris_detections = []
        
        if boxes is not None:
            for box in boxes:
                class_id = int(box.cls[0])
                confidence = float(box.conf[0])
                
                if class_id == 0:  # crack
                    crack_detections.append(confidence)
                elif class_id == 1:  # pothole
                    pothole_detections.append(confidence)
                elif class_id == 2:  # debris
                    debris_detections.append(confidence)
        
        return {
            'crack_confidence': max(crack_detections) if crack_detections else 0.0,
            'pothole_confidence': max(pothole_detections) if pothole_detections else 0.0,
            'pothole_count': len(pothole_detections),
            'debris_score': max(debris_detections) if debris_detections else 0.0,
            'surface_roughness': self._estimate_roughness(crack_detections + pothole_detections),
            'lane_visibility': 0.7,  # Would need separate detection
            'weather_condition': 'unknown',
            'confidence': 0.8
        }
    
    def _estimate_roughness(self, defect_scores) -> float:
        """Estimate surface roughness from defect detections"""
        if not defect_scores:
            return 0.1
        return min(0.9, sum(defect_scores) / len(defect_scores))
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get model information"""
        return {
            'model_type': 'YOLOv8RoadQuality',
            'version': '1.0.0',
            'framework': 'ultralytics',
            'input_shape': 'variable',
            'classes': list(self.class_names.values()),
            'is_loaded': self.is_loaded
        }