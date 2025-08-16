#!/usr/bin/env python3
"""
Demo script for coordinate-based image fetching and analysis with database integration.

This script demonstrates how to use the DatabasePipeline to:
- Fetch street-level images from coordinates
- Analyze them for quality and road conditions
- Save all results to PostgreSQL database
- Handle duplicate detection automatically

Focuses on Edinburgh city locations with database persistence.

Usage:
    python demo_coordinate_fetching.py

Requirements:
    - MAPILLARY_ACCESS_TOKEN environment variable set
    - PostgreSQL database configured (see .env file)
    - Required dependencies installed (see requirements.txt)

Output:
    - Creates 'edinburgh_road_analysis' directory
    - Downloads images to subdirectories by location
    - Saves all data to PostgreSQL database
    - Generates summary JSON files
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path


# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.services.pipeline.database_pipeline import DatabasePipeline


def serialize_pipeline_result(pipeline_result):
    """Convert PipelineResult to JSON-serializable dictionary."""
    result_dict = {
        "image_path": pipeline_result.image_path,
        "processed_successfully": pipeline_result.processed_successfully,
        "processing_time_ms": pipeline_result.processing_time_ms,
        "timestamp": pipeline_result.timestamp,
        "pipeline_version": pipeline_result.pipeline_version,
    }

    # Add quality metrics
    if pipeline_result.quality_metrics:
        quality_metrics = pipeline_result.quality_metrics
        result_dict["quality_metrics"] = {
            "overall_score": quality_metrics.overall_score,
            "is_usable": quality_metrics.is_usable,
            "timestamp": quality_metrics.timestamp,
            "assessment_version": getattr(quality_metrics, "assessment_version", "1.0.0"),
            "blur_score": quality_metrics.blur_score,
            "exposure_score": quality_metrics.exposure_score,
            "size_score": quality_metrics.size_score,
            "road_surface_percentage": quality_metrics.road_surface_percentage,
            "has_sufficient_road": quality_metrics.has_sufficient_road,
            "failure_reasons": [reason.value for reason in quality_metrics.failure_reasons]
            if quality_metrics.failure_reasons
            else [],
        }

    # Add road metrics
    if pipeline_result.road_metrics:
        road_metrics = pipeline_result.road_metrics
        result_dict["road_metrics"] = {
            "overall_quality_score": road_metrics.overall_quality_score,
            "crack_confidence": road_metrics.crack_confidence,
            "crack_severity": road_metrics.crack_severity,
            "pothole_confidence": road_metrics.pothole_confidence,
            "pothole_count": road_metrics.pothole_count,
            "surface_roughness": road_metrics.surface_roughness,
            "lane_marking_visibility": road_metrics.lane_marking_visibility,
            "debris_score": road_metrics.debris_score,
            "weather_condition": road_metrics.weather_condition,
            "assessment_confidence": road_metrics.assessment_confidence,
            "timestamp": road_metrics.timestamp,
            "model_name": road_metrics.model_name,
            "model_version": road_metrics.model_version,
        }

    return result_dict


def demo_coordinate_analysis():
    """Demonstrate coordinate-based image fetching and analysis."""

    # Check for Mapillary API token
    if not os.getenv("MAPILLARY_ACCESS_TOKEN"):
        print("❌ Error: MAPILLARY_ACCESS_TOKEN environment variable not set")
        print("   Please get a token from https://www.mapillary.com/developer")
        print("   and set it as: export MAPILLARY_ACCESS_TOKEN=your_token_here")
        return None

    print("Edinburgh Road Quality Analysis Demo")
    print("=" * 50)

    # Create output directory
    output_base = Path("edinburgh_road_analysis")
    output_base.mkdir(exist_ok=True)

    # Initialize database pipeline with image fetching enabled
    print("🔧 Initializing database pipeline...")
    try:
        pipeline = DatabasePipeline(enable_fetcher=True)
        print("✅ Database pipeline initialized successfully")

        # Display service information
        info = pipeline.get_service_info()
        print(f"📋 Pipeline version: {info['pipeline_version']}")
        print(f"📋 Fetcher available: {info['fetcher_service'] is not None}")

        # Show database connection status
        db_stats = pipeline.get_database_stats()
        print(
            f"📊 Database connected: {db_stats.get('total_photos', 0)} photos already in database"
        )

    except Exception as e:
        print(f"❌ Failed to initialize pipeline: {e}")
        return None

    # Edinburgh locations - key areas within the city
    edinburgh_locations = [
        {
            "name": "Edinburgh Castle & Royal Mile",
            "description": "Historic Royal Mile near Edinburgh Castle",
            "lat": 55.9486,
            "lon": -3.1999,
        },
        {
            "name": "Princes Street",
            "description": "Main shopping street in New Town",
            "lat": 55.9520,
            "lon": -3.1964,
        },
        {
            "name": "Leith Walk",
            "description": "Major road connecting city center to Leith",
            "lat": 55.9615,
            "lon": -3.1729,
        },
        {
            "name": "Morningside Road",
            "description": "Residential area in south Edinburgh",
            "lat": 55.9282,
            "lon": -3.2073,
        },
        {
            "name": "Easter Road",
            "description": "Main road in Leith area",
            "lat": 55.9612,
            "lon": -3.1615,
        },
    ]

    # Session metadata
    session_start = datetime.now()
    session_metadata = {
        "session_id": f"edinburgh_analysis_{session_start.strftime('%Y%m%d_%H%M%S')}",
        "start_time": session_start.isoformat(),
        "pipeline_version": info["pipeline_version"],
        "locations_analyzed": len(edinburgh_locations),
        "city": "Edinburgh",
        "country": "Scotland",
    }

    all_results = {}

    for i, location in enumerate(edinburgh_locations, 1):
        location_name = location["name"].replace(" ", "_").replace("&", "and").lower()
        location_dir = output_base / f"{i:02d}_{location_name}"

        print(f"\n📍 Location {i}/5: {location['name']}")
        print(f"   {location['description']}")
        print(f"   Coordinates: {location['lat']:.4f}, {location['lon']:.4f}")
        print("   Search radius: 5m")

        try:
            # Fetch and analyze images at this location with database integration
            result = pipeline.process_coordinate_with_db(
                lat=location["lat"],
                lon=location["lon"],
                radius_m=5,  # Constant 5m radius for all locations
                limit=5,  # Get up to 5 images per location
                output_dir=str(location_dir),
            )

            # Store results from database pipeline
            location_key = f"location_{i:02d}_{location_name}"
            all_results[location_key] = {
                "location_info": location,
                "fetch_result": result["fetch_result"],
                "database_summary": result["summary"],
                "processed_images": result["processed_images"],
            }

            # Display results
            fetch_result = result["fetch_result"]
            summary = result["summary"]

            print(f"   🔍 Found: {fetch_result['images_found']} images")
            print(f"   ⬇️  Downloaded: {fetch_result['images_downloaded']} images")
            print(f"   ⚡ Fetch time: {fetch_result['processing_time_ms']:.1f}ms")

            if summary["total_processed"] > 0:
                print(f"   📊 Images processed: {summary['total_processed']}")
                print(f"   💾 Saved to database: {summary['successful_database_saves']}")
                print(f"   🔄 Duplicates found: {summary['duplicates_found']}")

                # Show sample results from processed images
                processed_images = result["processed_images"]
                successful_images = [img for img in processed_images if img.get("database_saved")]

                for j, image_result in enumerate(
                    successful_images[:3], 1
                ):  # Show max 3 sample results
                    if "pipeline_result" in image_result:
                        pipeline_result = image_result["pipeline_result"]
                        print(
                            f"   📸 Image {j}: Database ID {image_result.get('database_ids', {}).get('photo_id', 'N/A')}"
                        )
                        if pipeline_result.processed_successfully:
                            quality_score = pipeline_result.quality_metrics.overall_score
                            if pipeline_result.road_metrics:
                                road_score = pipeline_result.road_metrics.overall_quality_score
                                print(f"      Quality: {quality_score:.1f}, Road: {road_score:.1f}")
                            else:
                                print(
                                    f"      Quality: {quality_score:.1f}, Road: Failed quality check"
                                )
                        else:
                            print("      Processing failed")

                # Save location-specific JSON results
                location_json_file = location_dir / f"{location_key}_results.json"
                with open(location_json_file, "w") as f:
                    json.dump(all_results[location_key], f, indent=2, default=str)
                print(f"   💾 Results saved to: {location_json_file}")
                print(
                    f"   💾 Database records: {summary['successful_database_saves']} photos stored"
                )

            else:
                print("   ⚠️  No images found at this location")

        except Exception as e:
            print(f"   ❌ Error processing location: {e}")
            # Still save error info
            location_key = f"location_{i:02d}_{location_name}"
            all_results[location_key] = {
                "location_info": location,
                "error": str(e),
                "success": False,
            }

    # Calculate overall summary statistics
    session_end = datetime.now()
    session_metadata.update(
        {
            "end_time": session_end.isoformat(),
            "total_duration_seconds": (session_end - session_start).total_seconds(),
            "status": "completed",
        }
    )

    # Aggregate statistics across all locations (database-integrated results)
    total_images_found = sum(
        result.get("fetch_result", {}).get("images_found", 0)
        for result in all_results.values()
        if "fetch_result" in result
    )
    total_images_downloaded = sum(
        result.get("fetch_result", {}).get("images_downloaded", 0)
        for result in all_results.values()
        if "fetch_result" in result
    )
    total_processed = sum(
        result.get("database_summary", {}).get("total_processed", 0)
        for result in all_results.values()
        if "database_summary" in result
    )
    total_saved_to_db = sum(
        result.get("database_summary", {}).get("successful_database_saves", 0)
        for result in all_results.values()
        if "database_summary" in result
    )
    total_duplicates = sum(
        result.get("database_summary", {}).get("duplicates_found", 0)
        for result in all_results.values()
        if "database_summary" in result
    )

    # Get final database statistics
    final_db_stats = pipeline.get_database_stats()

    overall_summary = {
        "session_metadata": session_metadata,
        "overall_stats": {
            "locations_processed": len(edinburgh_locations),
            "total_images_found": total_images_found,
            "total_images_downloaded": total_images_downloaded,
            "total_images_processed": total_processed,
            "total_saved_to_database": total_saved_to_db,
            "total_duplicates_found": total_duplicates,
            "database_save_rate": (total_saved_to_db / total_processed * 100)
            if total_processed > 0
            else 0,
        },
        "database_totals": final_db_stats,
        "detailed_results": all_results,
    }

    # Save comprehensive results
    master_json_file = (
        output_base / f"edinburgh_road_analysis_{session_start.strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(master_json_file, "w") as f:
        json.dump(overall_summary, f, indent=2, default=str)

    print("\n🎉 Edinburgh road analysis completed!")
    print(f"📁 All results saved to: {output_base}")
    print(f"📄 Master results file: {master_json_file}")
    print(f"💾 Database save rate: {overall_summary['overall_stats']['database_save_rate']:.1f}%")
    print(f"📊 Total images processed: {total_processed} ({total_saved_to_db} saved to database)")
    print(f"🔄 Duplicate images found: {total_duplicates}")
    print(
        f"📊 Database totals: {final_db_stats.get('total_photos', 0)} photos, {final_db_stats.get('road_analyzed', 0)} road analyses"
    )

    return overall_summary


if __name__ == "__main__":
    print("🎯 Edinburgh Road Quality Analysis Demo (Database-Integrated)")
    print("This demo fetches street images from Edinburgh coordinates, analyzes them,")
    print("and saves all results to PostgreSQL database with duplicate detection.")
    print("Results are also saved as JSON files for backup/analysis.")
    print()

    # Run main demo
    results = demo_coordinate_analysis()

    if results:
        print("\n📋 Session Summary:")
        print(f"   📊 Locations processed: {results['overall_stats']['locations_processed']}")
        print(f"   📸 Total images found: {results['overall_stats']['total_images_found']}")
        print(
            f"   ⬇️  Total images downloaded: {results['overall_stats']['total_images_downloaded']}"
        )
        print(f"   ⚙️  Total images processed: {results['overall_stats']['total_images_processed']}")
        print(f"   💾 Saved to database: {results['overall_stats']['total_saved_to_database']}")
        print(f"   🔄 Duplicates found: {results['overall_stats']['total_duplicates_found']}")
        print(f"   📈 Database save rate: {results['overall_stats']['database_save_rate']:.1f}%")
        print(
            f"   ⏱️  Total session time: {results['session_metadata']['total_duration_seconds']:.1f} seconds"
        )
        print(
            f"   📊 Final database: {results['database_totals'].get('total_photos', 0)} photos total"
        )

    print("\n📖 For technical details, see tests/test_coordinate_fetching.py")
    print("📁 All images and JSON results are saved in the 'edinburgh_road_analysis' directory")
