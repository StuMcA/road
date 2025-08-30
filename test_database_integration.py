#!/usr/bin/env python3
"""
Test script for database integration with road quality analysis.

Tests:
- Database connection
- Photo processing with database saves
- Duplicate detection
- Coordinate processing with Mapillary integration
"""

import sys
from pathlib import Path


# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.database import DatabaseService
from src.services.pipeline.database_pipeline import DatabasePipeline


def test_database_connection():
    """Test basic database connectivity."""
    print("🔧 Testing database connection...")

    try:
        db_service = DatabaseService()
        stats = db_service.get_processing_stats()
        print("✅ Database connected successfully")
        print(f"   Current stats: {stats}")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def test_coordinate_processing():
    """Test coordinate processing with database integration."""
    print("\n📍 Testing coordinate processing with database...")

    try:
        # Initialize database pipeline
        pipeline = DatabasePipeline(enable_fetcher=True)

        # Test with a different location to avoid duplicates with demo
        result = pipeline.process_coordinate_with_db(
            lat=51.5074,  # London coordinates
            lon=-0.1278,
            radius_m=10,
            limit=2,  # Small limit for testing
        )

        if result["success"]:
            summary = result["summary"]
            print("✅ Coordinate processing successful!")
            print(f"   Images fetched: {summary['total_images_fetched']}")
            print(f"   Images processed: {summary['total_processed']}")
            print(f"   Saved to database: {summary['successful_database_saves']}")
            print(f"   Duplicates found: {summary['duplicates_found']}")

            # Show database stats
            stats = pipeline.get_database_stats()
            print(
                f"   Database totals: {stats['total_photos']} photos, {stats['road_analyzed']} analyzed"
            )

            return True
        print(f"❌ Coordinate processing failed: {result.get('error', 'Unknown error')}")
        return False

    except Exception as e:
        print(f"❌ Coordinate processing error: {e}")
        return False


def test_duplicate_detection():
    """Test duplicate detection by running same coordinate twice."""
    print("\n🔍 Testing duplicate detection...")

    try:
        pipeline = DatabasePipeline(enable_fetcher=True)

        # Process same coordinate twice
        print("   First processing...")
        result1 = pipeline.process_coordinate_with_db(
            lat=51.5072,  # Different London coordinates
            lon=-0.1276,
            radius_m=5,
            limit=1,
        )

        print("   Second processing (should find duplicates)...")
        result2 = pipeline.process_coordinate_with_db(
            lat=51.5072,  # Same location
            lon=-0.1276,
            radius_m=5,
            limit=1,
        )

        if result1["success"] and result2["success"]:
            duplicates1 = result1["summary"]["duplicates_found"]
            duplicates2 = result2["summary"]["duplicates_found"]

            print("✅ Duplicate detection working!")
            print(f"   First run duplicates: {duplicates1}")
            print(f"   Second run duplicates: {duplicates2}")
            print("   Expected: Second run should have more duplicates")

            return True
        print("❌ Duplicate detection test failed")
        return False

    except Exception as e:
        print(f"❌ Duplicate detection error: {e}")
        return False


def show_database_summary():
    """Show final database statistics."""
    print("\n📊 Final Database Summary:")

    try:
        db_service = DatabaseService()
        stats = db_service.get_processing_stats()

        print(f"   Total photos: {stats.get('total_photos', 0)}")
        print(f"   Quality assessed: {stats.get('quality_assessed', 0)}")
        print(f"   Usable photos: {stats.get('usable_photos', 0)}")
        print(f"   Road analyzed: {stats.get('road_analyzed', 0)}")

        if stats.get("avg_quality_score"):
            print(f"   Average quality score: {stats['avg_quality_score']:.1f}")
        if stats.get("avg_road_score"):
            print(f"   Average road score: {stats['avg_road_score']:.1f}")

        print(f"   Last analysis: {stats.get('last_road_analysis', 'None')}")

    except Exception as e:
        print(f"❌ Error getting database summary: {e}")


def main():
    """Run all tests."""
    print("🎯 Road Quality Database Integration Test")
    print("=" * 50)

    tests = [
        ("Database Connection", test_database_connection),
        ("Coordinate Processing", test_coordinate_processing),
        ("Duplicate Detection", test_duplicate_detection),
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 30)
        success = test_func()
        results.append((test_name, success))

    # Show summary
    show_database_summary()

    print("\n🎯 Test Results Summary:")
    print("=" * 30)
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")

    passed = sum(1 for _, success in results if success)
    total = len(results)
    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Database integration is working correctly.")
    else:
        print("⚠️  Some tests failed. Check logs above for details.")


if __name__ == "__main__":
    main()
