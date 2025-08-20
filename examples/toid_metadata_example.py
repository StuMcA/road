#!/usr/bin/env python3
"""
Example usage of the streamlined TOID metadata service

This demonstrates the new 3-step workflow:
1. Features API: Fetch TOIDs and coordinates
2. Names API: Get street/location metadata for each coordinate  
3. Database: Save combined data
"""

import logging
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from services.toid_metadata_service import TOIDMetadataService

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_process_area():
    """Example: Process an area to get TOIDs with street metadata."""
    
    # Define a bounding box (example: area in Edinburgh)
    bbox = [-3.205, 55.948, -3.200, 55.952]  # [min_lon, min_lat, max_lon, max_lat]
    
    logger.info("Example: Processing area for TOID metadata")
    logger.info(f"Bounding box: {bbox}")
    
    # Initialize service
    service = TOIDMetadataService()
    
    # Process the area (limit to 10 TOIDs for this example)
    results = service.process_area(bbox=bbox, max_toids=10)
    
    # Display results
    print(f"\n{'='*50}")
    print("PROCESSING RESULTS")
    print(f"{'='*50}")
    print(f"Success: {results['success']}")
    print(f"TOIDs fetched from Features API: {results['toids_fetched']}")
    print(f"TOIDs with street metadata: {results['toids_with_metadata']}")
    print(f"TOIDs saved to database: {results['toids_saved']}")
    print(f"Errors: {len(results['errors'])}")
    
    if results['errors']:
        print("\nErrors:")
        for error in results['errors']:
            print(f"  - {error}")
    
    return results


def example_search_database():
    """Example: Search for TOIDs in the database."""
    
    logger.info("Example: Searching database for TOIDs")
    
    service = TOIDMetadataService()
    
    # Search in a specific area
    bbox = [-3.205, 55.948, -3.200, 55.952]
    toids = service.search_toids_by_location(bbox=bbox, limit=5)
    
    print(f"\n{'='*50}")
    print("DATABASE SEARCH RESULTS")
    print(f"{'='*50}")
    print(f"Found {len(toids)} TOIDs in database:")
    
    for toid in toids:
        print(f"\nTOID: {toid['toid']}")
        print(f"  Location: {toid['latitude']:.6f}, {toid['longitude']:.6f}")
        print(f"  Street: {toid.get('street_name', 'Unknown')}")
        print(f"  Locality: {toid.get('locality', 'Unknown')}")
        print(f"  Region: {toid.get('region', 'Unknown')}")
        print(f"  Postcode Area: {toid.get('postcode_area', 'Unknown')}")
        print(f"  Created: {toid['created_at']}")


def example_get_single_toid():
    """Example: Get metadata for a specific TOID."""
    
    logger.info("Example: Getting single TOID metadata")
    
    service = TOIDMetadataService()
    
    # First get some TOIDs to work with
    bbox = [-3.205, 55.948, -3.200, 55.952]
    toids = service.search_toids_by_location(bbox=bbox, limit=1)
    
    if not toids:
        print("No TOIDs found in database to demonstrate with")
        return
    
    # Get detailed metadata for the first TOID
    toid_id = toids[0]['toid']
    metadata = service.get_toid_metadata(toid_id)
    
    print(f"\n{'='*50}")
    print("SINGLE TOID METADATA")
    print(f"{'='*50}")
    if metadata:
        print(f"TOID: {metadata['toid']}")
        print(f"Version Date: {metadata['version_date']}")
        print(f"Source: {metadata['source_product']}")
        print(f"Location: {metadata['latitude']:.6f}, {metadata['longitude']:.6f}")
        print(f"Street Name: {metadata.get('street_name', 'Unknown')}")
        print(f"Locality: {metadata.get('locality', 'Unknown')}")
        print(f"Region: {metadata.get('region', 'Unknown')}")
        print(f"Postcode Area: {metadata.get('postcode_area', 'Unknown')}")
        print(f"Last Updated: {metadata['updated_at']}")
    else:
        print(f"TOID {toid_id} not found")


def main():
    """Run all examples."""
    
    print("TOID Metadata Service Examples")
    print("=" * 60)
    
    try:
        # Example 1: Process an area
        print("\n1. PROCESSING AREA FOR TOID METADATA")
        print("-" * 40)
        results = example_process_area()
        
        # Example 2: Search database (only if we have data)
        if results and results.get('toids_saved', 0) > 0:
            print("\n2. SEARCHING DATABASE")
            print("-" * 40) 
            example_search_database()
            
            print("\n3. GETTING SINGLE TOID METADATA")
            print("-" * 40)
            example_get_single_toid()
        else:
            print("\nSkipping database examples - no data was saved")
        
        print(f"\n{'='*60}")
        print("All examples completed successfully!")
        
    except Exception as e:
        logger.error(f"Examples failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()