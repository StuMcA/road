#!/usr/bin/env python3
"""
Test script for the road quality service with sample images
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from services.road_quality import RoadQualityService

def test_road_quality_service():
    """Test the road quality service with sample images"""
    print("Testing Road Quality Service")
    print("=" * 40)
    
    # Find sample images
    sample_dir = Path("../mapillary_images")
    if not sample_dir.exists():
        print(f"❌ Sample images directory not found: {sample_dir}")
        return
    
    # Look for image files
    image_files = list(sample_dir.glob("*.jpg")) + list(sample_dir.glob("*.png"))
    
    if not image_files:
        print("❌ No sample images found in mapillary_images/")
        return
    
    print(f"📁 Found {len(image_files)} sample images")
    
    try:
        # Initialize service
        print("🔧 Initializing road quality service...")
        service = RoadQualityService()
        
        # Test with first few images
        test_images = image_files[:3]
        
        for i, image_path in enumerate(test_images, 1):
            print(f"\n📸 Testing image {i}: {image_path.name}")
            
            try:
                # Assess road quality
                metrics = service.assess_road_quality(str(image_path))
                
                if metrics:
                    print(f"  ✅ Analysis complete")
                    print(f"  🎯 Quality Score: {metrics.overall_quality_score:.1f}/100")
                    print(f"  🔍 Confidence: {metrics.assessment_confidence:.2f}")
                    print(f"  🕳️  Crack Severity: {metrics.crack_severity}")
                    print(f"  ⚡ Pothole Count: {metrics.pothole_count}")
                    print(f"  🌧️  Weather: {metrics.weather_condition}")
                else:
                    print("  ❌ Failed to analyze image")
                    
            except Exception as e:
                print(f"  ❌ Error: {e}")
        
        # Test batch processing
        print(f"\n📦 Testing batch processing with {len(test_images)} images...")
        try:
            results = service.batch_assess([str(img) for img in test_images])
            successful = sum(1 for r in results.values() if r is not None)
            print(f"  ✅ Batch complete: {successful}/{len(test_images)} successful")
        except Exception as e:
            print(f"  ❌ Batch processing error: {e}")
        
        print("\n✨ Road quality service test complete!")
        
    except Exception as e:
        print(f"❌ Service initialization error: {e}")
        print("💡 Make sure ultralytics is installed: pip install ultralytics")

if __name__ == "__main__":
    test_road_quality_service()