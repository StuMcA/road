#!/usr/bin/env python3
"""
Full Dataset Road Analysis Pipeline

Processes all 49,312 street points with 8m radius for high precision road quality analysis.
Expected runtime: ~4.7 hours
Expected success rate: ~0-2% (but highly accurate results)
"""

import logging
import sys
import time
from datetime import datetime
from typing import List, Tuple

from src.database.services.database_service import DatabaseService
from src.services.pipeline.database_pipeline import DatabasePipeline

# Configure comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('full_dataset_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_all_unprocessed_street_points(db_service: DatabaseService) -> List[Tuple[int, float, float]]:
    """Get all street points that haven't been processed yet."""
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


def process_batch_with_8m_radius(
    pipeline: DatabasePipeline,
    street_points: List[Tuple[int, float, float]], 
    batch_start: int,
    batch_size: int = 50
) -> dict:
    """Process a batch of street points with 8m radius."""
    batch_end = min(batch_start + batch_size, len(street_points))
    batch = street_points[batch_start:batch_end]
    
    batch_results = {
        'processed': 0,
        'successful': 0,  
        'no_images': 0,
        'failed': 0,
        'total_images': 0,
        'total_saved': 0,
        'errors': []
    }
    
    for street_point_id, lat, lon in batch:
        try:
            result = pipeline.process_coordinate_with_db(
                lat=lat,
                lon=lon,
                radius_m=8.0,  # High precision 8m radius
                limit=5,
                output_dir=None
            )
            
            batch_results['processed'] += 1
            
            if result.get('success'):
                summary = result.get('summary', {})
                images_found = summary.get('total_images_fetched', 0)
                images_saved = summary.get('successful_database_saves', 0)
                
                if images_found > 0:
                    batch_results['successful'] += 1
                    batch_results['total_images'] += images_found
                    batch_results['total_saved'] += images_saved
                    logger.info(f"✓ Point {street_point_id}: {images_found} images, {images_saved} saved")
                else:
                    batch_results['no_images'] += 1
            else:
                batch_results['failed'] += 1
                error_msg = result.get('error', 'Unknown error')
                batch_results['errors'].append((street_point_id, error_msg))
                logger.warning(f"✗ Point {street_point_id}: {error_msg}")
                
        except Exception as e:
            batch_results['failed'] += 1
            error_msg = str(e)
            batch_results['errors'].append((street_point_id, error_msg))
            logger.error(f"✗ Point {street_point_id}: Exception - {error_msg}")
    
    return batch_results


def main():
    """Run full dataset analysis."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("STARTING FULL DATASET ROAD ANALYSIS PIPELINE")
    logger.info("Configuration: 8m radius, high precision mode")
    logger.info(f"Start time: {start_time}")
    logger.info("Expected runtime: ~4.7 hours")
    logger.info("="*80)
    
    try:
        # Initialize services
        logger.info("Initializing services...")
        db_service = DatabaseService()
        pipeline = DatabasePipeline(enable_fetcher=True, database_service=db_service)
        
        # Get all unprocessed street points
        logger.info("Loading all unprocessed street points...")
        street_points = get_all_unprocessed_street_points(db_service)
        total_points = len(street_points)
        
        logger.info(f"Total street points to process: {total_points}")
        
        if total_points == 0:
            logger.info("No unprocessed street points found. Exiting.")
            return
        
        # Process in batches for progress tracking and memory efficiency
        batch_size = 50
        total_batches = (total_points + batch_size - 1) // batch_size
        
        logger.info(f"Processing in {total_batches} batches of {batch_size}")
        
        # Overall statistics
        overall_stats = {
            'processed': 0,
            'successful': 0,
            'no_images': 0, 
            'failed': 0,
            'total_images': 0,
            'total_saved': 0,
            'total_errors': 0
        }
        
        # Process each batch
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_start_time = time.time()
            
            batch_results = process_batch_with_8m_radius(
                pipeline, street_points, batch_start, batch_size
            )
            
            batch_duration = time.time() - batch_start_time
            
            # Update overall stats
            for key in ['processed', 'successful', 'no_images', 'failed', 'total_images', 'total_saved']:
                overall_stats[key] += batch_results[key]
            overall_stats['total_errors'] += len(batch_results['errors'])
            
            # Progress reporting
            progress_pct = ((batch_num + 1) / total_batches) * 100
            points_processed = overall_stats['processed']
            
            # Calculate ETA
            elapsed_total = (datetime.now() - start_time).total_seconds()
            if points_processed > 0:
                avg_time_per_point = elapsed_total / points_processed
                remaining_points = total_points - points_processed
                eta_seconds = remaining_points * avg_time_per_point
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime('%H:%M:%S')
            else:
                eta_str = "Calculating..."
            
            logger.info(f"Batch {batch_num + 1}/{total_batches} completed in {batch_duration:.1f}s")
            logger.info(f"Progress: {progress_pct:.1f}% ({points_processed}/{total_points})")
            logger.info(f"Found images: {overall_stats['successful']} points, {overall_stats['total_images']} images")
            logger.info(f"ETA: {eta_str}")
            logger.info("-" * 50)
            
            # Brief pause between batches
            time.sleep(0.5)
    
        # Final results
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("="*80)
        logger.info("FULL DATASET ANALYSIS COMPLETED")
        logger.info(f"Total duration: {duration}")
        logger.info("="*80)
        logger.info("FINAL STATISTICS:")
        logger.info(f"Street points processed: {overall_stats['processed']}")
        logger.info(f"Points with images found: {overall_stats['successful']}")
        logger.info(f"Points with no images: {overall_stats['no_images']}")
        logger.info(f"Processing failures: {overall_stats['failed']}")
        logger.info(f"Total images found: {overall_stats['total_images']}")
        logger.info(f"Total images saved to DB: {overall_stats['total_saved']}")
        logger.info(f"Total processing errors: {overall_stats['total_errors']}")
        
        if overall_stats['processed'] > 0:
            success_rate = (overall_stats['successful'] / overall_stats['processed']) * 100
            logger.info(f"Success rate: {success_rate:.3f}%")
            
            if overall_stats['successful'] > 0:
                avg_images = overall_stats['total_images'] / overall_stats['successful']
                logger.info(f"Average images per successful point: {avg_images:.1f}")
        
        # Final database statistics
        logger.info("Final database state:")
        db_stats = pipeline.get_database_stats()
        for key, value in db_stats.items():
            logger.info(f"  {key}: {value}")
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        logger.info(f"Processed {overall_stats.get('processed', 0)} points before interruption")
    except Exception as e:
        logger.error(f"Fatal error in pipeline execution: {e}")
        raise


if __name__ == "__main__":
    from datetime import timedelta
    main()