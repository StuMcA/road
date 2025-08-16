#!/usr/bin/env python3
"""
Debug script to understand why images are failing quality checks
"""

import sys
from pathlib import Path


# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from services.image_quality import ImageQualityService
from services.image_quality.heuristics import check_image_quality


def debug_image_quality():
    """Debug image quality assessment"""
    print("🔍 Debugging Image Quality Assessment")
    print("=" * 50)

    # Find sample images
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if not image_files:
        print("❌ No sample images found")
        return

    service = ImageQualityService()

    for image_file in image_files:
        print(f"\n📸 Analyzing: {image_file.name}")
        print("-" * 30)

        # Get raw heuristics
        raw_heuristics = check_image_quality(str(image_file))

        print("Raw Heuristics:")
        print(f"  Blurry: {raw_heuristics['blurry'][0]} (score: {raw_heuristics['blurry'][1]:.2f})")
        print(
            f"  Poor exposure: {raw_heuristics['poor_exposure'][0]} (values: {raw_heuristics['poor_exposure'][1]})"
        )
        print(
            f"  Too small: {raw_heuristics['too_small'][0]} (size: {raw_heuristics['too_small'][1]})"
        )
        print(f"  Usable: {raw_heuristics['usable']}")

        # Get full quality assessment
        quality_result = service.evaluate(str(image_file))

        print("\nQuality Service Results:")
        print(f"  Overall score: {quality_result.overall_score:.2f}/100")
        print(f"  Blur score: {quality_result.blur_score:.2f}/100")
        print(f"  Exposure score: {quality_result.exposure_score:.2f}/100")
        print(f"  Size score: {quality_result.size_score:.2f}/100")
        print(f"  Road surface: {quality_result.road_surface_percentage:.2f}%")
        print(f"  Is usable: {quality_result.is_usable}")

        if quality_result.failure_reasons:
            print("  Failure reasons:")
            for reason in quality_result.failure_reasons:
                print(f"    • {reason.display_message}")


def test_blur_threshold():
    """Test different blur thresholds"""
    print("\n🔍 Testing Blur Threshold Sensitivity")
    print("=" * 50)

    import cv2

    from services.image_quality.heuristics import is_blurry

    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if not image_files:
        return

    # Test different thresholds
    thresholds = [50, 75, 100, 125, 150, 200]

    for image_file in image_files[:2]:  # Test first 2 images
        print(f"\n📸 {image_file.name}:")

        image = cv2.imread(str(image_file))
        if image is None:
            continue

        for threshold in thresholds:
            is_blur, lap_var = is_blurry(image, threshold)
            status = "❌ BLURRY" if is_blur else "✅ SHARP"
            print(f"  Threshold {threshold:3d}: {status} (variance: {lap_var:.2f})")


if __name__ == "__main__":
    debug_image_quality()
    test_blur_threshold()
