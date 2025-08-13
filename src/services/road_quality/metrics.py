from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime
import numpy as np

@dataclass
class RoadQualityMetrics:
    overall_quality_score: float  # 0-100, higher is better
    crack_confidence: float  # 0-1, confidence of crack detection
    crack_severity: str  # 'none', 'minor', 'moderate', 'severe'
    pothole_confidence: float  # 0-1, confidence of pothole detection
    pothole_count: int  # estimated number of potholes
    surface_roughness: float  # 0-1, 0=smooth, 1=very rough
    lane_marking_visibility: float  # 0-1, visibility of lane markings
    debris_score: float  # 0-1, amount of debris/obstacles on road
    weather_condition: str  # 'dry', 'wet', 'unknown'
    assessment_confidence: float  # 0-1, overall model confidence
    timestamp: str  # ISO format timestamp of assessment
    model_name: str  # Name/version of model used
    model_version: str  # Version of the model
    
    def to_dict(self) -> Dict[str, Any]:
        def convert_numpy(val):
            """Convert numpy types to native Python types for JSON serialization"""
            if isinstance(val, (np.integer, np.floating)):
                return val.item()
            elif isinstance(val, np.ndarray):
                return val.tolist()
            return val
        
        return {
            'overall_quality_score': convert_numpy(self.overall_quality_score),
            'crack_detection': {
                'confidence': convert_numpy(self.crack_confidence),
                'severity': self.crack_severity
            },
            'pothole_detection': {
                'confidence': convert_numpy(self.pothole_confidence),
                'count': convert_numpy(self.pothole_count)
            },
            'surface_roughness': convert_numpy(self.surface_roughness),
            'lane_marking_visibility': convert_numpy(self.lane_marking_visibility),
            'debris_score': convert_numpy(self.debris_score),
            'weather_condition': self.weather_condition,
            'assessment_confidence': convert_numpy(self.assessment_confidence),
            'metadata': {
                'timestamp': self.timestamp,
                'model_name': self.model_name,
                'model_version': self.model_version
            }
        }
    
    @classmethod
    def from_model_output(cls, model_predictions: Dict[str, Any], model_info: Dict[str, Any]) -> 'RoadQualityMetrics':
        """Create metrics from raw model predictions"""
        crack_conf = model_predictions.get('crack_confidence', 0.0)
        pothole_conf = model_predictions.get('pothole_confidence', 0.0)
        roughness = model_predictions.get('surface_roughness', 0.0)
        
        # Determine crack severity based on confidence
        if crack_conf < 0.2:
            crack_severity = 'none'
        elif crack_conf < 0.5:
            crack_severity = 'minor'
        elif crack_conf < 0.8:
            crack_severity = 'moderate'
        else:
            crack_severity = 'severe'
        
        # Calculate overall quality score (inverse relationship with problems)
        quality_score = 100 * (1 - max(crack_conf, pothole_conf, roughness))
        
        def safe_float(val):
            """Convert to native Python float"""
            if isinstance(val, (np.integer, np.floating)):
                return float(val.item())
            return float(val)
        
        def safe_int(val):
            """Convert to native Python int"""
            if isinstance(val, (np.integer, np.floating)):
                return int(val.item())
            return int(val)
        
        return cls(
            overall_quality_score=safe_float(max(0.0, min(100.0, quality_score))),
            crack_confidence=safe_float(crack_conf),
            crack_severity=crack_severity,
            pothole_confidence=safe_float(pothole_conf),
            pothole_count=safe_int(model_predictions.get('pothole_count', 0)),
            surface_roughness=safe_float(roughness),
            lane_marking_visibility=safe_float(model_predictions.get('lane_visibility', 0.5)),
            debris_score=safe_float(model_predictions.get('debris_score', 0.0)),
            weather_condition=model_predictions.get('weather_condition', 'unknown'),
            assessment_confidence=safe_float(model_predictions.get('confidence', 0.5)),
            timestamp=datetime.now().isoformat(),
            model_name=model_info.get('model_type', 'unknown'),
            model_version=model_info.get('version', 'unknown')
        )