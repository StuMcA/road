#!/usr/bin/env python3
"""
Test script to show metadata output
"""

import sys
import json
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from services.road_quality import RoadQualityService

def test_metadata():
    """Test metadata output"""
    print("Testing Road Quality Service Metadata")
    print("=" * 40)
    
    # Find sample image
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))
    
    if not image_files:
        print("❌ No sample images found")
        return
    
    try:
        # Initialize service
        service = RoadQualityService()
        
        # Test with one image
        image_path = str(image_files[0])
        print(f"📸 Analyzing: {image_files[0].name}")
        
        metrics = service.assess_road_quality(image_path)
        
        if metrics:
            print("\n📊 Full JSON Output:")
            print(json.dumps(metrics.to_dict(), indent=2))
            
            print(f"\n🕒 Timestamp: {metrics.timestamp}")
            print(f"🤖 Model: {metrics.model_name}")
            print(f"📌 Version: {metrics.model_version}")
        else:
            print("❌ Failed to analyze image")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_metadata()
    