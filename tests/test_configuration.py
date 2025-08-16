#!/usr/bin/env python3
"""
Test script to demonstrate configuration system for thresholds and parameters
"""

import os
import sys
from pathlib import Path


# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from config import QualityConfig
from services.image_quality import ImageQualityService


def test_default_config():
    """Test with default configuration"""
    print("🔧 Testing Default Configuration")
    print("=" * 40)

    config = QualityConfig()
    service = ImageQualityService(config)

    print("Default Settings:")
    print(f"  Blur threshold: {config.blur_threshold}")
    print(f"  Min image size: {config.min_width}x{config.min_height}")
    print(f"  Dark threshold: {config.dark_threshold}")
    print(f"  Bright threshold: {config.bright_threshold}")
    print(f"  Min road surface: {config.min_road_surface_percentage}%")

    # Test with sample image
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if image_files:
        result = service.evaluate(str(image_files[0]))
        print("\nResult with default config:")
        print(f"  Overall score: {result.overall_score:.1f}/100")
        print(f"  Usable: {result.is_usable}")
        if result.failure_reasons:
            print(f"  Failures: {[r.value for r in result.failure_reasons]}")


def test_env_config():
    """Test configuration from environment variables"""
    print("\n🔧 Testing Environment Variable Configuration")
    print("=" * 40)

    # Set some environment variables
    os.environ["BLUR_THRESHOLD"] = "25.0"
    os.environ["MIN_ROAD_SURFACE"] = "15.0"
    os.environ["DARK_THRESHOLD"] = "0.3"

    config = QualityConfig.from_env()
    service = ImageQualityService(config)

    print("Environment Settings:")
    print(f"  Blur threshold: {config.blur_threshold} (from BLUR_THRESHOLD)")
    print(f"  Min road surface: {config.min_road_surface_percentage}% (from MIN_ROAD_SURFACE)")
    print(f"  Dark threshold: {config.dark_threshold} (from DARK_THRESHOLD)")

    # Test with sample image
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if image_files:
        result = service.evaluate(str(image_files[0]))
        print("\nResult with env config:")
        print(f"  Overall score: {result.overall_score:.1f}/100")
        print(f"  Usable: {result.is_usable}")
        if result.failure_reasons:
            print(f"  Failures: {[r.value for r in result.failure_reasons]}")

    # Clean up environment
    del os.environ["BLUR_THRESHOLD"]
    del os.environ["MIN_ROAD_SURFACE"]
    del os.environ["DARK_THRESHOLD"]


def test_custom_config():
    """Test with custom configuration values"""
    print("\n🔧 Testing Custom Configuration")
    print("=" * 40)

    # Create custom config
    config = QualityConfig(
        blur_threshold=20.0,  # Very lenient blur
        min_road_surface_percentage=10.0,  # Very low road requirement
        dark_threshold=0.5,  # Very lenient exposure
        bright_threshold=0.95,
    )

    service = ImageQualityService(config)

    print("Custom Settings (very lenient):")
    print(f"  Blur threshold: {config.blur_threshold}")
    print(f"  Min road surface: {config.min_road_surface_percentage}%")
    print(f"  Dark threshold: {config.dark_threshold}")
    print(f"  Bright threshold: {config.bright_threshold}")

    # Test with sample image
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if image_files:
        result = service.evaluate(str(image_files[0]))
        print("\nResult with custom config:")
        print(f"  Overall score: {result.overall_score:.1f}/100")
        print(f"  Usable: {result.is_usable}")
        if result.failure_reasons:
            print(f"  Failures: {[r.value for r in result.failure_reasons]}")


def test_config_comparison():
    """Compare results across different configurations"""
    print("\n🔧 Testing Configuration Impact")
    print("=" * 40)

    # Find sample image
    sample_dir = Path("../mapillary_images")
    image_files = list(sample_dir.glob("*.png"))

    if not image_files:
        print("No sample images found")
        return

    image_path = str(image_files[0])
    configs = {
        "Strict": QualityConfig(blur_threshold=100.0, min_road_surface_percentage=30.0),
        "Default": QualityConfig(),
        "Lenient": QualityConfig(blur_threshold=10.0, min_road_surface_percentage=5.0)
    }

    print(f"Testing image: {image_files[0].name}")
    print(f"{'Config':<12} {'Score':<8} {'Usable':<8} {'Failures'}")
    print("-" * 50)

    for config_name, config in configs.items():
        service = ImageQualityService(config)
        result = service.evaluate(image_path)

        failures = (
            ", ".join([r.value for r in result.failure_reasons])
            if result.failure_reasons
            else "None"
        )
        print(
            f"{config_name:<12} {result.overall_score:<8.1f} {str(result.is_usable):<8} {failures}"
        )


def main():
    """Run all configuration tests"""
    print("Configuration System Tests")
    print("=" * 50)

    test_default_config()
    test_env_config()
    test_custom_config()
    test_config_comparison()

    print("\n✨ Configuration testing complete!")
    print("💡 Use different configs for production vs testing vs debugging")


if __name__ == "__main__":
    main()
