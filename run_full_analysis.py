#!/usr/bin/env python3
"""
Full Road Analysis Pipeline Runner

Processes all street points in the database through the complete pipeline:
1. Fetch images from Mapillary API for each street point
2. Run quality analysis on all images
3. Run road quality analysis on images that pass quality checks
4. Save all results back to the database

This script handles:
- Batch processing with progress tracking
- Error handling and recovery
- Rate limiting compliance
- Database transaction safety
- Comprehensive logging
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
        logging.FileHandler('road_analysis_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_all_street_points(db_service: DatabaseService) -> List[Tuple[int, float, float]]:
    """Get all street points from database."""
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, ST_Y(location::geometry) as latitude, ST_X(location::geometry) as longitude 
        FROM street_points 
        ORDER BY id
    """)
    
    points = [(row['id'], row['latitude'], row['longitude']) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return points


def get_unprocessed_street_points(db_service: DatabaseService) -> List[Tuple[int, float, float]]:
    """Get street points that haven't been processed yet (no photos linked)."""
    conn = db_service.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sp.id, ST_Y(sp.location::geometry) as latitude, ST_X(sp.location::geometry) as longitude 
        FROM street_points sp
        LEFT JOIN photos p ON p.street_point_id = sp.id
        WHERE p.id IS NULL
        ORDER BY sp.id
    """)
    
    points = [(row['id'], row['latitude'], row['longitude']) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return points


def process_street_point_batch(
    pipeline: DatabasePipeline,
    street_points: List[Tuple[int, float, float]],
    batch_start: int,
    batch_size: int = 10
) -> dict:
    """Process a batch of street points."""
    batch_end = min(batch_start + batch_size, len(street_points))
    batch = street_points[batch_start:batch_end]
    
    logger.info(f"Processing batch {batch_start//batch_size + 1}: points {batch_start+1}-{batch_end}")
    
    batch_results = {
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'no_images': 0,
        'errors': []
    }
    
    for street_point_id, lat, lon in batch:
        try:
            logger.info(f"Processing street point {street_point_id}: ({lat:.6f}, {lon:.6f})")
            
            # Process coordinate with database integration
            result = pipeline.process_coordinate_with_db(
                lat=lat,
                lon=lon,
                radius_m=100.0,
                limit=5,  # Limit images per point to manage processing time
                output_dir=None  # Use temp directory
            )
            
            batch_results['processed'] += 1
            
            if result.get('success'):
                summary = result.get('summary', {})
                if summary.get('total_images_fetched', 0) > 0:
                    batch_results['successful'] += 1
                    logger.info(f"Street point {street_point_id}: {summary.get('total_images_fetched')} images, "
                              f"{summary.get('successful_database_saves')} saved to DB")
                else:
                    batch_results['no_images'] += 1
                    logger.info(f"Street point {street_point_id}: No images found")
            else:
                batch_results['failed'] += 1
                error_msg = result.get('error', 'Unknown error')
                logger.warning(f"Street point {street_point_id} failed: {error_msg}")
                batch_results['errors'].append((street_point_id, error_msg))
                
        except Exception as e:
            batch_results['failed'] += 1
            error_msg = f"Exception processing street point {street_point_id}: {e}"
            logger.error(error_msg)
            batch_results['errors'].append((street_point_id, str(e)))
    
    return batch_results


def main():
    """Main pipeline execution."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("STARTING FULL ROAD ANALYSIS PIPELINE")
    logger.info(f"Start time: {start_time}")
    logger.info("="*80)
    
    try:
        # Initialize services
        logger.info("Initializing services...")
        db_service = DatabaseService()
        pipeline = DatabasePipeline(enable_fetcher=True, database_service=db_service)
        
        # Get street points to process
        logger.info("Fetching street points from database...")
        street_points = get_unprocessed_street_points(db_service)
        total_points = len(street_points)
        
        logger.info(f"Found {total_points} unprocessed street points")
        
        if total_points == 0:
            logger.info("No unprocessed street points found. Exiting.")
            return
        
        # Process in batches
        batch_size = 10  # Process 10 street points at a time
        total_batches = (total_points + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total_points} street points in {total_batches} batches of {batch_size}")
        
        # Overall statistics
        overall_stats = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'no_images': 0,
            'total_errors': 0
        }
        
        # Process each batch
        for batch_start in range(0, total_points, batch_size):
            batch_results = process_street_point_batch(
                pipeline, street_points, batch_start, batch_size
            )
            
            # Update overall stats
            for key in ['processed', 'successful', 'failed', 'no_images']:
                overall_stats[key] += batch_results[key]
            overall_stats['total_errors'] += len(batch_results['errors'])
            
            # Progress report
            progress = (batch_start + batch_size) / total_points * 100
            logger.info(f"Progress: {min(progress, 100.0):.1f}% complete "
                       f"({overall_stats['processed']}/{total_points} processed)")
            
            # Brief pause between batches to be respectful to APIs
            time.sleep(1)
    
        # Final results
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("="*80)
        logger.info("PIPELINE COMPLETED")
        logger.info(f"End time: {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info("="*80)
        logger.info("FINAL STATISTICS:")
        logger.info(f"Street points processed: {overall_stats['processed']}")
        logger.info(f"Successfully processed: {overall_stats['successful']}")
        logger.info(f"Failed processing: {overall_stats['failed']}")
        logger.info(f"No images found: {overall_stats['no_images']}")
        logger.info(f"Total errors: {overall_stats['total_errors']}")
        
        success_rate = (overall_stats['successful'] / overall_stats['processed'] * 100) if overall_stats['processed'] > 0 else 0
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Get final database statistics
        logger.info("\nFinal database statistics:")
        db_stats = pipeline.get_database_stats()
        for key, value in db_stats.items():
            logger.info(f"{key}: {value}")
            
    except Exception as e:
        logger.error(f"Fatal error in pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()