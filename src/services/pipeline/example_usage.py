#!/usr/bin/env python3
"""
Example usage of the database-driven TOIDAnalysisPipeline.

This example demonstrates the two-step process:
1. Populate database with street data using StreetDataService
2. Process the data using TOIDAnalysisPipeline (database-only)
"""

from typing import List
from ..street_data_service import StreetDataService
from .toid_analysis_pipeline import TOIDAnalysisPipeline


def example_workflow(area_bbox: List[float], max_features: int = 50):
    """
    Example workflow showing proper usage of the database-driven pipeline.
    
    Args:
        area_bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
        max_features: Maximum features to process
    """
    
    print("=== Database-Driven TOID Analysis Pipeline Example ===")
    
    # Step 1: Populate database with street data
    print("\n1. Populating database with street data...")
    street_service = StreetDataService()
    
    collection_result = street_service.collect_street_data(
        bbox=area_bbox,
        max_features=max_features
    )
    
    if not collection_result["success"]:
        print(f"❌ Failed to collect street data: {collection_result.get('error', 'Unknown error')}")
        return
    
    print(f"✅ Collected {collection_result['features_saved']} street data entries")
    print(f"   - {collection_result['features_with_metadata']} entries have metadata")
    
    # Step 2: Initialize database-only analysis pipeline
    print("\n2. Initializing analysis pipeline (database-only mode)...")
    pipeline = TOIDAnalysisPipeline(
        bbox_radius_m=100.0,
        photos_per_bbox=5,
        enable_database=True
    )
    
    # Step 3: Check data availability
    print("\n3. Checking data availability...")
    availability = pipeline.check_data_availability(area_bbox)
    
    if not availability["data_available"]:
        print(f"❌ No data available: {availability['message']}")
        return
    
    print(f"✅ Data available: {availability['count']} entries found")
    
    # Step 4: Run analysis (will only use database data)
    print("\n4. Running image analysis pipeline...")
    analysis_result = pipeline.run_area_analysis(
        area_bbox=area_bbox,
        max_features=max_features,
        output_dir="./analysis_output"
    )
    
    if not analysis_result["success"]:
        error_type = analysis_result.get("error_type", "unknown")
        
        if error_type == "missing_data":
            print(f"❌ Analysis failed - missing data: {analysis_result['error']}")
            print(f"💡 Recommendation: {analysis_result['recommendation']}")
        else:
            print(f"❌ Analysis failed - system error: {analysis_result['error']}")
        return
    
    # Step 5: Display results
    print("\n5. Analysis Results:")
    session_summary = analysis_result.get("session_summary", {})
    print(f"✅ Analysis completed successfully!")
    print(f"   - Processed: {session_summary.get('photos_processed', 0)} photos")
    print(f"   - Saved: {session_summary.get('photos_saved', 0)} results")
    print(f"   - Success rate: {session_summary.get('success_rate', 0):.1f}%")
    print(f"   - Processing time: {session_summary.get('total_processing_time_ms', 0):.0f}ms")


def example_graceful_failure(empty_area_bbox: List[float]):
    """
    Example showing graceful failure when no data is available.
    
    Args:
        empty_area_bbox: Bounding box with no street data
    """
    print("\n=== Graceful Failure Example ===")
    
    # Try to run analysis on area with no data
    pipeline = TOIDAnalysisPipeline(enable_database=True)
    
    print("Attempting analysis on area with no street data...")
    result = pipeline.run_area_analysis(
        area_bbox=empty_area_bbox,
        max_features=10
    )
    
    if not result["success"]:
        error_type = result.get("error_type", "unknown")
        print(f"❌ Expected failure occurred: {error_type}")
        print(f"   Error: {result['error']}")
        if "recommendation" in result:
            print(f"   💡 {result['recommendation']}")
    else:
        print("⚠️  Unexpected: Analysis succeeded on empty area")


def example_database_statistics():
    """Example showing how to get database statistics."""
    print("\n=== Database Statistics Example ===")
    
    pipeline = TOIDAnalysisPipeline(enable_database=True)
    stats = pipeline.get_database_statistics()
    
    if stats["success"]:
        data = stats["statistics"]
        print(f"📊 Database contains:")
        print(f"   - Total street data entries: {data.get('total_street_data', 0)}")
        print(f"   - Entries with street names: {data.get('entries_with_street_name', 0)}")
        print(f"   - Unique street names: {data.get('unique_street_names', 0)}")
        print(f"   - Unique localities: {data.get('unique_localities', 0)}")
        
        if "geographical_bounds" in data:
            bounds = data["geographical_bounds"]
            print(f"   - Geographic coverage: {bounds.get('min_longitude', 0):.3f}, {bounds.get('min_latitude', 0):.3f} to {bounds.get('max_longitude', 0):.3f}, {bounds.get('max_latitude', 0):.3f}")
    else:
        print(f"❌ Failed to get statistics: {stats.get('error', 'Unknown error')}")


if __name__ == "__main__":
    # Example bounding box (small area in London)
    london_bbox = [-0.1276, 51.5074, -0.1256, 51.5094]
    
    # Example empty area (middle of ocean)
    empty_bbox = [0.0, 0.0, 0.001, 0.001]
    
    # Run examples
    example_workflow(london_bbox, max_features=20)
    example_graceful_failure(empty_bbox) 
    example_database_statistics()