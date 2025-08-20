#!/usr/bin/env python3
"""
Large-scale trial of the TOID metadata workflow across Edinburgh
Processes 1000 TOIDs and saves detailed results to file
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from clients.ordnance_survey.os_features_client import OSFeaturesClient
from clients.ordnance_survey.os_names_client import OSNamesClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_edinburgh_trial():
    """Run large-scale trial across Edinburgh with 1000 TOIDs."""
    
    # Larger Edinburgh area bounding box
    edinburgh_bbox = [-3.25, 55.92, -3.15, 55.98]  # Wider area covering more of Edinburgh
    
    logger.info("Starting Edinburgh large-scale trial")
    logger.info(f"Area: {edinburgh_bbox}")
    logger.info("Target: 1000 TOIDs")
    
    start_time = datetime.now()
    
    # Results structure
    results = {
        "trial_metadata": {
            "start_time": start_time.isoformat(),
            "area_bbox": edinburgh_bbox,
            "target_toids": 1000,
            "trial_type": "edinburgh_large_scale"
        },
        "summary": {},
        "individual_results": [],
        "errors": []
    }
    
    try:
        # Step 1: Fetch TOIDs from Features API
        logger.info("Step 1: Fetching TOIDs from Features API...")
        features_client = OSFeaturesClient()
        
        toid_features = features_client.fetch_all_toid_features(
            bbox=edinburgh_bbox,
            max_features=1000
        )
        
        logger.info(f"Fetched {len(toid_features)} TOIDs from Features API")
        results["summary"]["toids_fetched"] = len(toid_features)
        
        if not toid_features:
            logger.error("No TOIDs found in area")
            return results
        
        # Step 2: Process each TOID with Names API
        logger.info("Step 2: Processing TOIDs with Names API...")
        names_client = OSNamesClient()
        
        successful_lookups = 0
        with_street_data = 0
        with_postcode = 0
        with_region = 0
        
        for i, toid_feature in enumerate(toid_features):
            toid, version_date, source_product, geom_wkt, longitude, latitude, easting, northing = toid_feature
            
            if (i + 1) % 50 == 0:
                logger.info(f"Processing TOID {i+1}/{len(toid_features)}: {toid}")
            
            try:
                # Get street metadata using BNG coordinates
                street_metadata = names_client.get_street_metadata(
                    easting=easting,
                    northing=northing,
                    radius=100
                )
                
                successful_lookups += 1
                
                # Count data quality
                if street_metadata.get("NAME1"):
                    with_street_data += 1
                if street_metadata.get("POSTCODE"):
                    with_postcode += 1
                if street_metadata.get("REGION"):
                    with_region += 1
                
                # Store individual result
                individual_result = {
                    "toid": toid,
                    "bng_coordinates": {
                        "easting": easting,
                        "northing": northing
                    },
                    "wgs84_coordinates": {
                        "longitude": longitude,
                        "latitude": latitude
                    },
                    "version_date": version_date.isoformat() if version_date else None,
                    "source_product": source_product,
                    "street_metadata": street_metadata,
                    "has_street_name": bool(street_metadata.get("NAME1")),
                    "has_postcode": bool(street_metadata.get("POSTCODE")),
                    "has_region": bool(street_metadata.get("REGION"))
                }
                
                results["individual_results"].append(individual_result)
                
            except Exception as e:
                error_info = {
                    "toid": toid,
                    "error": str(e),
                    "coordinates": {"easting": easting, "northing": northing}
                }
                results["errors"].append(error_info)
                logger.debug(f"Error processing TOID {toid}: {e}")
        
        # Calculate summary statistics
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        results["trial_metadata"]["end_time"] = end_time.isoformat()
        results["trial_metadata"]["processing_time_seconds"] = processing_time
        
        results["summary"].update({
            "successful_api_calls": successful_lookups,
            "toids_with_street_names": with_street_data,
            "toids_with_postcodes": with_postcode,
            "toids_with_regions": with_region,
            "error_count": len(results["errors"]),
            "success_rate_percent": (successful_lookups / len(toid_features)) * 100 if toid_features else 0,
            "street_name_rate_percent": (with_street_data / successful_lookups) * 100 if successful_lookups else 0,
            "postcode_rate_percent": (with_postcode / successful_lookups) * 100 if successful_lookups else 0,
            "region_rate_percent": (with_region / successful_lookups) * 100 if successful_lookups else 0,
            "processing_time_minutes": processing_time / 60,
            "average_time_per_toid_ms": (processing_time * 1000) / len(toid_features) if toid_features else 0
        })
        
        logger.info("=" * 60)
        logger.info("EDINBURGH TRIAL COMPLETED")
        logger.info("=" * 60)
        logger.info(f"TOIDs processed: {len(toid_features)}")
        logger.info(f"Successful API calls: {successful_lookups}")
        logger.info(f"Success rate: {results['summary']['success_rate_percent']:.1f}%")
        logger.info(f"With street names: {with_street_data} ({results['summary']['street_name_rate_percent']:.1f}%)")
        logger.info(f"With postcodes: {with_postcode} ({results['summary']['postcode_rate_percent']:.1f}%)")
        logger.info(f"With regions: {with_region} ({results['summary']['region_rate_percent']:.1f}%)")
        logger.info(f"Processing time: {results['summary']['processing_time_minutes']:.1f} minutes")
        logger.info(f"Average per TOID: {results['summary']['average_time_per_toid_ms']:.0f}ms")
        logger.info(f"Errors: {len(results['errors'])}")
        
    except Exception as e:
        logger.error(f"Trial failed: {e}", exc_info=True)
        results["errors"].append({
            "type": "trial_failure",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })
    
    return results


def save_results(results):
    """Save results to JSON file with timestamp."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"edinburgh_trial_results_{timestamp}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Results saved to: {filename}")
        
        # Also save a summary-only file
        summary_filename = f"edinburgh_trial_summary_{timestamp}.json"
        summary_data = {
            "trial_metadata": results["trial_metadata"],
            "summary": results["summary"],
            "error_count": len(results.get("errors", [])),
            "sample_results": results.get("individual_results", [])[:5]  # First 5 as examples
        }
        
        with open(summary_filename, 'w') as f:
            json.dump(summary_data, f, indent=2, default=str)
        
        logger.info(f"Summary saved to: {summary_filename}")
        
        return filename, summary_filename
        
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
        return None, None


def main():
    """Main function to run the trial and save results."""
    
    # Check environment variables
    if not os.getenv("OS_API_KEY"):
        logger.error("OS_API_KEY environment variable not set")
        sys.exit(1)
    
    logger.info("Starting Edinburgh Large-Scale Trial")
    logger.info("=" * 60)
    
    # Run the trial
    results = run_edinburgh_trial()
    
    # Save results
    results_file, summary_file = save_results(results)
    
    if results_file:
        logger.info("=" * 60)
        logger.info("TRIAL COMPLETED SUCCESSFULLY")
        logger.info(f"Full results: {results_file}")
        logger.info(f"Summary: {summary_file}")
        
        # Print key statistics
        summary = results.get("summary", {})
        logger.info("")
        logger.info("KEY STATISTICS:")
        logger.info(f"  TOIDs processed: {summary.get('toids_fetched', 0)}")
        logger.info(f"  Success rate: {summary.get('success_rate_percent', 0):.1f}%")
        logger.info(f"  Street names found: {summary.get('street_name_rate_percent', 0):.1f}%")
        logger.info(f"  Postcodes found: {summary.get('postcode_rate_percent', 0):.1f}%")
        logger.info(f"  Regions found: {summary.get('region_rate_percent', 0):.1f}%")
    else:
        logger.error("Trial completed but failed to save results")
        sys.exit(1)


if __name__ == "__main__":
    main()