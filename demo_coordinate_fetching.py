#!/usr/bin/env python3
"""
Demo script for coordinate-based image fetching and analysis.

This script demonstrates how to use the enhanced RoadAnalysisPipeline
to fetch street-level images from coordinates and analyze them.
Focuses on Edinburgh city locations with JSON output.

Usage:
    python demo_coordinate_fetching.py

Requirements:
    - MAPILLARY_ACCESS_TOKEN environment variable set
    - Required dependencies installed (see requirements.txt)

Output:
    - Creates 'edinburgh_road_analysis' directory
    - Downloads images to subdirectories by location
    - Saves analysis results as JSON files
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.services.pipeline.road_analysis_pipeline import RoadAnalysisPipeline


def serialize_pipeline_result(pipeline_result):
    """Convert PipelineResult to JSON-serializable dictionary."""
    result_dict = {
        "image_path": pipeline_result.image_path,
        "processed_successfully": pipeline_result.processed_successfully,
        "processing_time_ms": pipeline_result.processing_time_ms,
        "timestamp": pipeline_result.timestamp,
        "pipeline_version": pipeline_result.pipeline_version
    }
    
    # Add quality metrics
    if pipeline_result.quality_metrics:
        quality_metrics = pipeline_result.quality_metrics
        result_dict["quality_metrics"] = {
            "overall_score": quality_metrics.overall_score,
            "is_usable": quality_metrics.is_usable,
            "timestamp": quality_metrics.timestamp,
            "assessment_version": getattr(quality_metrics, 'assessment_version', '1.0.0'),
            "blur_score": quality_metrics.blur_score,
            "exposure_score": quality_metrics.exposure_score,
            "size_score": quality_metrics.size_score,
            "road_surface_percentage": quality_metrics.road_surface_percentage,
            "has_sufficient_road": quality_metrics.has_sufficient_road,
            "failure_reasons": [reason.value for reason in quality_metrics.failure_reasons] if quality_metrics.failure_reasons else []
        }
        
    
    # Add road metrics
    if pipeline_result.road_metrics:
        road_metrics = pipeline_result.road_metrics
        result_dict["road_metrics"] = {
            "overall_quality_score": road_metrics.overall_quality_score,
            "crack_score": road_metrics.crack_score,
            "pothole_score": road_metrics.pothole_score,
            "debris_score": road_metrics.debris_score,
            "lane_marking_score": road_metrics.lane_marking_score,
            "processing_time_ms": road_metrics.processing_time_ms,
            "timestamp": road_metrics.timestamp,
            "model_version": road_metrics.model_version,
            "detections": [
                {
                    "class_name": det.class_name,
                    "confidence": det.confidence,
                    "bbox": det.bbox,
                    "area": det.area
                } for det in road_metrics.detections
            ] if road_metrics.detections else []
        }
    
    return result_dict


def demo_coordinate_analysis():
    """Demonstrate coordinate-based image fetching and analysis."""
    
    # Check for Mapillary API token
    if not os.getenv("MAPILLARY_ACCESS_TOKEN"):
        print("❌ Error: MAPILLARY_ACCESS_TOKEN environment variable not set")
        print("   Please get a token from https://www.mapillary.com/developer")
        print("   and set it as: export MAPILLARY_ACCESS_TOKEN=your_token_here")
        return
    
    print("Edinburgh Road Quality Analysis Demo")
    print("=" * 50)
    
    # Create output directory
    output_base = Path("edinburgh_road_analysis")
    output_base.mkdir(exist_ok=True)
    
    # Initialize pipeline with image fetching enabled
    print("🔧 Initializing pipeline...")
    try:
        pipeline = RoadAnalysisPipeline(enable_fetcher=True)
        print("✅ Pipeline initialized successfully")
        
        # Display service information
        info = pipeline.get_service_info()
        print(f"📋 Pipeline version: {info['pipeline_version']}")
        print(f"📋 Fetcher available: {info['fetcher_service'] is not None}")
        
    except Exception as e:
        print(f"❌ Failed to initialize pipeline: {e}")
        return
    
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
        }
    ]
    
    # Session metadata
    session_start = datetime.now()
    session_metadata = {
        "session_id": f"edinburgh_analysis_{session_start.strftime('%Y%m%d_%H%M%S')}",
        "start_time": session_start.isoformat(),
        "pipeline_version": info['pipeline_version'],
        "locations_analyzed": len(edinburgh_locations),
        "city": "Edinburgh",
        "country": "Scotland"
    }
    
    all_results = {}
    summary_stats = {}
    
    for i, location in enumerate(edinburgh_locations, 1):
        location_name = location['name'].replace(' ', '_').replace('&', 'and').lower()
        location_dir = output_base / f"{i:02d}_{location_name}"
        
        print(f"\n📍 Location {i}/5: {location['name']}")
        print(f"   {location['description']}")
        print(f"   Coordinates: {location['lat']:.4f}, {location['lon']:.4f}")
        print(f"   Search radius: 2m")
        
        try:
            # Fetch and analyze images at this location
            result = pipeline.process_coordinate(
                lat=location['lat'],
                lon=location['lon'],
                radius_m=2,  # Constant 2m radius for all locations
                limit=5,  # Get up to 5 images per location
                output_dir=str(location_dir)
            )
            
            # Store results
            location_key = f"location_{i:02d}_{location_name}"
            all_results[location_key] = {
                "location_info": location,
                "fetch_result": result['fetch_result'],
                "analysis_summary": result['analysis_summary'],
                "pipeline_results": {}
            }
            
            # Convert pipeline results to JSON-serializable format
            for image_path, pipeline_result in result['pipeline_results'].items():
                image_key = Path(image_path).stem
                all_results[location_key]["pipeline_results"][image_key] = serialize_pipeline_result(pipeline_result)
            
            # Display results
            fetch_result = result['fetch_result']
            analysis_summary = result['analysis_summary']
            
            print(f"   🔍 Found: {fetch_result['images_found']} images")
            print(f"   ⬇️  Downloaded: {fetch_result['images_downloaded']} images")
            print(f"   ⚡ Fetch time: {fetch_result['processing_time_ms']:.1f}ms")
            
            if fetch_result['images_downloaded'] > 0:
                print(f"   📊 Analysis success rate: {analysis_summary['success_rate']:.1f}%")
                print(f"   📊 Images analyzed: {analysis_summary['total_images']}")
                print(f"   📊 Successful analyses: {analysis_summary['successful_analyses']}")
                
                # Show sample results
                pipeline_results = result['pipeline_results']
                for j, (image_path, pipeline_result) in enumerate(pipeline_results.items(), 1):
                    if j > 3:  # Show max 3 sample results
                        break
                    
                    print(f"   📸 Image {j}: {Path(image_path).name}")
                    if pipeline_result.processed_successfully:
                        quality_score = pipeline_result.quality_metrics.overall_score
                        road_score = pipeline_result.road_metrics.overall_quality_score
                        print(f"      Quality: {quality_score:.2f}, Road: {road_score:.2f}")
                    else:
                        print(f"      Failed quality check")
                
                # Save location-specific JSON results
                location_json_file = location_dir / f"{location_key}_results.json"
                with open(location_json_file, 'w') as f:
                    json.dump(all_results[location_key], f, indent=2, default=str)
                print(f"   💾 Results saved to: {location_json_file}")
                
            else:
                print("   ⚠️  No images found at this location")
                
        except Exception as e:
            print(f"   ❌ Error processing location: {e}")
            # Still save error info
            location_key = f"location_{i:02d}_{location_name}"
            all_results[location_key] = {
                "location_info": location,
                "error": str(e),
                "success": False
            }
    
    # Calculate overall summary statistics
    session_end = datetime.now()
    session_metadata.update({
        "end_time": session_end.isoformat(),
        "total_duration_seconds": (session_end - session_start).total_seconds(),
        "status": "completed"
    })
    
    # Aggregate statistics across all locations
    total_images_found = sum(result.get('fetch_result', {}).get('images_found', 0) for result in all_results.values() if 'fetch_result' in result)
    total_images_downloaded = sum(result.get('fetch_result', {}).get('images_downloaded', 0) for result in all_results.values() if 'fetch_result' in result)
    total_successful_analyses = sum(result.get('analysis_summary', {}).get('successful_analyses', 0) for result in all_results.values() if 'analysis_summary' in result)
    total_analyzed = sum(result.get('analysis_summary', {}).get('total_images', 0) for result in all_results.values() if 'analysis_summary' in result)
    
    overall_summary = {
        "session_metadata": session_metadata,
        "overall_stats": {
            "locations_processed": len(edinburgh_locations),
            "total_images_found": total_images_found,
            "total_images_downloaded": total_images_downloaded,
            "total_images_analyzed": total_analyzed,
            "total_successful_analyses": total_successful_analyses,
            "overall_success_rate": (total_successful_analyses / total_analyzed * 100) if total_analyzed > 0 else 0
        },
        "detailed_results": all_results
    }
    
    # Save comprehensive results
    master_json_file = output_base / f"edinburgh_road_analysis_{session_start.strftime('%Y%m%d_%H%M%S')}.json"
    with open(master_json_file, 'w') as f:
        json.dump(overall_summary, f, indent=2, default=str)
    
    print(f"\n🎉 Edinburgh road analysis completed!")
    print(f"📁 All results saved to: {output_base}")
    print(f"📄 Master results file: {master_json_file}")
    print(f"📊 Overall success rate: {overall_summary['overall_stats']['overall_success_rate']:.1f}%")
    print(f"📊 Total images analyzed: {total_analyzed} ({total_successful_analyses} successful)")
    
    return overall_summary


if __name__ == "__main__":
    print("🎯 Edinburgh Road Quality Analysis Demo")
    print("This demo fetches street images from Edinburgh coordinates and analyzes them")
    print("Results are saved as both images and comprehensive JSON analysis files")
    print()
    
    # Run main demo
    results = demo_coordinate_analysis()
    
    if results:
        print("\n📋 Session Summary:")
        print(f"   📊 Locations processed: {results['overall_stats']['locations_processed']}")
        print(f"   📸 Total images found: {results['overall_stats']['total_images_found']}")
        print(f"   ⬇️  Total images downloaded: {results['overall_stats']['total_images_downloaded']}")
        print(f"   ✅ Successful analyses: {results['overall_stats']['total_successful_analyses']}")
        print(f"   📈 Overall success rate: {results['overall_stats']['overall_success_rate']:.1f}%")
        print(f"   ⏱️  Total session time: {results['session_metadata']['total_duration_seconds']:.1f} seconds")
    
    print("\n📖 For technical details, see tests/test_coordinate_fetching.py")
    print("📁 All images and JSON results are saved in the 'edinburgh_road_analysis' directory")