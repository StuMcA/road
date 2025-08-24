#!/usr/bin/env python3
"""
Targeted Road Analysis Pipeline Runner

Focuses on street points most likely to have Mapillary coverage:
- Edinburgh city center coordinates (55.94-55.96 lat, -3.22 to -3.18 lon)
- 8m search radius for precise nearby image discovery
- Smaller batch processing for monitoring
"""

import logging
import sys
import time
from datetime import datetime
from typing import List, Tuple

from src.database.services.database_service import DatabaseService
from src.services.pipeline.database_pipeline import DatabasePipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('targeted_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_city_center_street_points(db_service: DatabaseService, limit: int = 500) -> List[Tuple[int, float, float]]:
    """Get street points from Edinburgh city center (most likely to have images)."""
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sp.id, ST_Y(sp.location::geometry) as latitude, ST_X(sp.location::geometry) as longitude 
        FROM street_points sp
        LEFT JOIN photos p ON p.street_point_id = sp.id
        WHERE p.id IS NULL
        AND ST_Y(sp.location::geometry) BETWEEN 55.94 AND 55.96
        AND ST_X(sp.location::geometry) BETWEEN -3.22 AND -3.18
        ORDER BY sp.id
        LIMIT %s
    """, (limit,))
    
    points = [(row['id'], row['latitude'], row['longitude']) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return points


def process_street_point_batch_targeted(
    pipeline: DatabasePipeline,
    street_points: List[Tuple[int, float, float]],
    batch_start: int,
    batch_size: int = 5
) -> dict:
    """Process a batch with 8m radius."""
    batch_end = min(batch_start + batch_size, len(street_points))
    batch = street_points[batch_start:batch_end]
    
    logger.info(f"Processing batch {batch_start//batch_size + 1}: points {batch_start+1}-{batch_end}")
    
    batch_results = {
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'no_images': 0,
        'total_images_found': 0,
        'total_images_saved': 0,
        'errors': []
    }
    
    for street_point_id, lat, lon in batch:
        try:
            logger.info(f"Processing street point {street_point_id}: ({lat:.6f}, {lon:.6f})")
            
            # Process coordinate with 8m radius
            result = pipeline.process_coordinate_with_db(
                lat=lat,
                lon=lon,
                radius_m=8.0,  # 8m radius as requested
                limit=5,
                output_dir=None
            )
            
            batch_results['processed'] += 1
            
            if result.get('success'):
                summary = result.get('summary', {})
                images_fetched = summary.get('total_images_fetched', 0)
                images_saved = summary.get('successful_database_saves', 0)
                
                if images_fetched > 0:
                    batch_results['successful'] += 1
                    batch_results['total_images_found'] += images_fetched
                    batch_results['total_images_saved'] += images_saved
                    logger.info(f"✓ Street point {street_point_id}: {images_fetched} images found, "
                              f"{images_saved} saved to DB")
                else:
                    batch_results['no_images'] += 1
                    logger.info(f"○ Street point {street_point_id}: No images found")
            else:
                batch_results['failed'] += 1
                error_msg = result.get('error', 'Unknown error')
                logger.warning(f"✗ Street point {street_point_id} failed: {error_msg}")
                batch_results['errors'].append((street_point_id, error_msg))
                
        except Exception as e:
            batch_results['failed'] += 1
            error_msg = f"Exception processing street point {street_point_id}: {e}"
            logger.error(f"✗ {error_msg}")
            batch_results['errors'].append((street_point_id, str(e)))
    
    return batch_results


def main():
    """Main targeted pipeline execution."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("STARTING TARGETED ROAD ANALYSIS PIPELINE")
    logger.info("Focusing on Edinburgh city center (55.94-55.96, -3.22 to -3.18)")
    logger.info("Using 8m search radius")
    logger.info(f"Start time: {start_time}")
    logger.info("="*80)
    
    try:
        # Initialize services
        logger.info("Initializing services...")
        db_service = DatabaseService()
        pipeline = DatabasePipeline(enable_fetcher=True, database_service=db_service)
        
        # Get targeted street points
        logger.info("Fetching city center street points...")
        street_points = get_city_center_street_points(db_service, limit=100)
        total_points = len(street_points)
        
        logger.info(f"Found {total_points} city center street points")
        
        if total_points == 0:
            logger.info("No city center street points found. Exiting.")
            return
        
        # Process in smaller batches
        batch_size = 5
        total_batches = (total_points + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total_points} street points in {total_batches} batches of {batch_size}")
        
        # Overall statistics
        overall_stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'no_images': 0,
            'total_images_found': 0,
            'total_images_saved': 0,
            'total_errors': 0
        }
        
        # Process each batch
        for batch_start in range(0, total_points, batch_size):
            batch_results = process_street_point_batch_targeted(
                pipeline, street_points, batch_start, batch_size
            )
            
            # Update overall stats
            for key in ['processed', 'successful', 'failed', 'no_images', 'total_images_found', 'total_images_saved']:
                overall_stats[key] += batch_results[key]
            overall_stats['total_errors'] += len(batch_results['errors'])
            
            # Progress report
            progress = (batch_start + batch_size) / total_points * 100
            logger.info(f"Progress: {min(progress, 100.0):.1f}% - "
                       f"Processed: {overall_stats['processed']}/{total_points}, "
                       f"Found images: {overall_stats['successful']}, "
                       f"Total images: {overall_stats['total_images_found']}")
            
            # Brief pause between batches
            time.sleep(1)
    
        # Final results
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("="*80)
        logger.info("TARGETED PIPELINE COMPLETED")
        logger.info(f"Duration: {duration}")
        logger.info("="*80)
        logger.info("FINAL STATISTICS:")
        logger.info(f"Street points processed: {overall_stats['processed']}")
        logger.info(f"Points with images found: {overall_stats['successful']}")
        logger.info(f"Points with no images: {overall_stats['no_images']}")
        logger.info(f"Failed processing: {overall_stats['failed']}")
        logger.info(f"Total images found: {overall_stats['total_images_found']}")
        logger.info(f"Total images saved to DB: {overall_stats['total_images_saved']}")
        
        success_rate = (overall_stats['successful'] / overall_stats['processed'] * 100) if overall_stats['processed'] > 0 else 0
        logger.info(f"Success rate (found images): {success_rate:.1f}%")
        
        if overall_stats['total_images_found'] > 0:
            avg_images_per_successful_point = overall_stats['total_images_found'] / overall_stats['successful']
            logger.info(f"Average images per successful point: {avg_images_per_successful_point:.1f}")
            
    except Exception as e:
        logger.error(f"Fatal error in pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()