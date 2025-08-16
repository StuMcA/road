#!/usr/bin/env python3
"""
Test script for the complete road analysis pipeline
Tests integration of image quality and road quality services
"""

import json
import sys
from pathlib import Path


# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from services.pipeline import RoadAnalysisPipeline


def test_pipeline_single_image():
    """Test pipeline with single image"""
    print("🔧 Testing Single Image Pipeline")
    print("=" * 40)

    # Find sample image
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if not image_files:
        print("❌ No sample images found")
        return

    try:
        # Initialize pipeline
        pipeline = RoadAnalysisPipeline()

        # Test with first image
        image_path = str(image_files[0])
        print(f"📸 Processing: {image_files[0].name}")

        # Process image
        result = pipeline.process_image(image_path)

        # Display results
        print(f"\n{result.summary}")
        print(f"⏱️  Processing time: {result.processing_time_ms:.1f}ms")
        print(f"✅ Quality passed: {result.quality_metrics.is_usable}")

        if result.processed_successfully:
            print(f"🎯 Quality score: {result.quality_metrics.overall_score:.1f}/100")
            print(f"🛣️  Road score: {result.road_metrics.overall_quality_score:.1f}/100")
            print(f"🚗 Road surface: {result.quality_metrics.road_surface_percentage:.1f}%")
        else:
            print("❌ Failure reasons:")
            for reason in result.quality_metrics.failure_reasons:
                print(f"   • {reason.display_message}")

        print("\n📊 Detailed JSON:")
        print(json.dumps(result.to_dict(), indent=2))

    except Exception as e:
        print(f"❌ Pipeline error: {e}")


def test_pipeline_batch():
    """Test pipeline with batch processing"""
    print("\n🔧 Testing Batch Pipeline Processing")
    print("=" * 40)

    # Find sample images
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if not image_files:
        print("❌ No sample images found")
        return

    try:
        # Initialize pipeline
        pipeline = RoadAnalysisPipeline()

        # Process all images
        image_paths = [str(img) for img in image_files]
        print(f"📁 Processing {len(image_paths)} images...")

        results = pipeline.process_batch(image_paths)

        # Display individual results
        print("\n📋 Individual Results:")
        for image_path, result in results.items():
            image_name = Path(image_path).name
            print(f"  {image_name}: {result.summary}")

        # Generate statistics
        stats = pipeline.get_pipeline_stats(results)

        print("\n📊 Batch Statistics:")
        print(f"  Total images: {stats['total_images']}")
        print(f"  Successful: {stats['successful_analyses']}")
        print(f"  Failed quality: {stats['failed_quality_check']}")
        print(f"  Success rate: {stats['success_rate']:.1f}%")
        print(f"  Avg processing: {stats['processing_times']['average_total_ms']:.1f}ms")
        print(f"  Avg quality score: {stats['quality_scores']['average']:.1f}/100")
        print(f"  Avg road score: {stats['road_scores']['average']:.1f}/100")

        # Performance comparison
        successful_time = stats["processing_times"]["average_successful_ms"]
        failed_time = stats["processing_times"]["average_failed_ms"]

        if failed_time > 0:
            speedup = successful_time / failed_time
            print(f"  ⚡ Quality filtering speedup: {speedup:.1f}x faster rejection")

    except Exception as e:
        print(f"❌ Batch processing error: {e}")


def test_pipeline_service_info():
    """Test pipeline service information"""
    print("\n🔧 Testing Pipeline Service Info")
    print("=" * 40)

    try:
        pipeline = RoadAnalysisPipeline()
        service_info = pipeline.get_service_info()

        print("📋 Pipeline Components:")
        print(f"  Pipeline version: {service_info['pipeline_version']}")
        print(f"  Quality service: v{service_info['quality_service']['version']}")
        print(
            f"  AI segmentation: {'✅' if service_info['quality_service']['segmentation_available'] else '❌'}"
        )
        print(f"  Road service: v{service_info['road_service']['service_version']}")
        print(f"  Road model: {service_info['road_service']['model_info']['model_type']}")

    except Exception as e:
        print(f"❌ Service info error: {e}")


def test_quality_gating():
    """Test quality gating with artificially bad image"""
    print("\n🔧 Testing Quality Gating Logic")
    print("=" * 40)

    try:
        pipeline = RoadAnalysisPipeline()

        # Test with non-existent file (should fail quality check)
        print("📸 Testing with non-existent file...")
        bad_result = pipeline.process_image("nonexistent.jpg")

        print(f"  Result: {bad_result.summary}")
        print(f"  Processed successfully: {bad_result.processed_successfully}")
        print(f"  Road analysis performed: {bad_result.road_metrics is not None}")
        print(f"  Processing time: {bad_result.processing_time_ms:.1f}ms (should be very fast)")

        # Show that quality gating prevents expensive road analysis
        if not bad_result.processed_successfully and bad_result.road_metrics is None:
            print("  ✅ Quality gating working: No road analysis on bad image")
        else:
            print("  ❌ Quality gating failed: Road analysis ran on bad image")

    except Exception as e:
        print(f"❌ Quality gating test error: {e}")


def main():
    """Run all pipeline tests"""
    print("Road Analysis Pipeline - Integration Tests")
    print("=" * 50)

    # Test individual components
    test_pipeline_single_image()
    test_pipeline_batch()
    test_pipeline_service_info()
    test_quality_gating()

    print("\n✨ Pipeline testing complete!")
    print("💡 The pipeline ensures only quality images reach expensive road analysis")


if __name__ == "__main__":
    main()
