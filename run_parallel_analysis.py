#!/usr/bin/env python3
"""
Parallel Road Analysis Pipeline

Parallelizes street point processing using ThreadPoolExecutor while respecting:
- Mapillary API rate limits (600 req/min = 10 req/sec)
- Database connection pooling
- Memory management

Estimated speedup: 5-8x faster than sequential processing
"""

import logging
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Tuple

from src.database.services.database_service import DatabaseService
from src.services.pipeline.database_pipeline import DatabasePipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parallel_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe rate limiter for API calls."""
    
    def __init__(self, max_requests_per_minute: int = 600):
        self.max_requests = max_requests_per_minute
        self.requests_times = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        with self.lock:
            now = time.time()
            # Remove requests older than 1 minute
            self.requests_times = [t for t in self.requests_times if now - t < 60]
            
            if len(self.requests_times) >= self.max_requests:
                # Calculate wait time
                oldest_request = min(self.requests_times)
                wait_time = 60 - (now - oldest_request) + 0.1  # Small buffer
                if wait_time > 0:
                    time.sleep(wait_time)
            
            self.requests_times.append(now)


# Global rate limiter
rate_limiter = RateLimiter(max_requests_per_minute=500)  # Conservative limit


def process_single_street_point(
    point_data: Tuple[int, float, float],
    db_service: DatabaseService
) -> dict:
    """Process a single street point with rate limiting."""
    street_point_id, lat, lon = point_data
    
    try:
        # Rate limiting
        rate_limiter.wait_if_needed()
        
        # Create pipeline instance for this thread
        pipeline = DatabasePipeline(enable_fetcher=True, database_service=db_service)
        
        result = pipeline.process_coordinate_with_db(
            lat=lat,
            lon=lon,
            radius_m=8.0,
            limit=5,
            output_dir=None
        )
        
        if result.get('success'):
            summary = result.get('summary', {})
            images_found = summary.get('total_images_fetched', 0)
            images_saved = summary.get('successful_database_saves', 0)
            
            return {
                'street_point_id': street_point_id,
                'status': 'success',
                'images_found': images_found,
                'images_saved': images_saved,
                'error': None
            }
        else:
            return {
                'street_point_id': street_point_id,
                'status': 'failed',
                'images_found': 0,
                'images_saved': 0,
                'error': result.get('error', 'Unknown error')
            }
            
    except Exception as e:
        return {
            'street_point_id': street_point_id,
            'status': 'exception',
            'images_found': 0,
            'images_saved': 0,
            'error': str(e)
        }


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


def process_batch_parallel(
    street_points: List[Tuple[int, float, float]],
    batch_start: int,
    batch_size: int,
    max_workers: int,
    db_service: DatabaseService
) -> dict:
    """Process a batch of street points in parallel."""
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
    
    # Process batch in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all points in the batch
        future_to_point = {
            executor.submit(process_single_street_point, point, db_service): point 
            for point in batch
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_point):
            result = future.result()
            batch_results['processed'] += 1
            
            if result['status'] == 'success':
                if result['images_found'] > 0:
                    batch_results['successful'] += 1
                    batch_results['total_images'] += result['images_found']
                    batch_results['total_saved'] += result['images_saved']
                    logger.info(f"✓ Point {result['street_point_id']}: "
                              f"{result['images_found']} images, {result['images_saved']} saved")
                else:
                    batch_results['no_images'] += 1
            else:
                batch_results['failed'] += 1
                batch_results['errors'].append((result['street_point_id'], result['error']))
                if result['status'] == 'exception':
                    logger.error(f"✗ Point {result['street_point_id']}: {result['error']}")
    
    return batch_results


def main():
    """Run parallel dataset analysis."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("STARTING PARALLEL ROAD ANALYSIS PIPELINE")
    logger.info("Configuration: 8m radius, parallel processing")
    logger.info(f"Start time: {start_time}")
    logger.info("="*80)
    
    try:
        # Initialize database service
        logger.info("Initializing database service...")
        db_service = DatabaseService()
        
        # Get all unprocessed street points
        logger.info("Loading all unprocessed street points...")
        street_points = get_all_unprocessed_street_points(db_service)
        total_points = len(street_points)
        
        logger.info(f"Total street points to process: {total_points}")
        
        if total_points == 0:
            logger.info("No unprocessed street points found. Exiting.")
            return
        
        # Configuration
        max_workers = 4  # Parallel threads (reduced to avoid resource conflicts)
        batch_size = 50  # Smaller batches to avoid memory issues
        total_batches = (total_points + batch_size - 1) // batch_size
        
        logger.info(f"Parallel configuration:")
        logger.info(f"  Max workers: {max_workers}")
        logger.info(f"  Batch size: {batch_size}")
        logger.info(f"  Total batches: {total_batches}")
        logger.info(f"  Rate limit: 500 requests/minute")
        
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
        
        # Process each batch in parallel
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_start_time = time.time()
            
            batch_results = process_batch_parallel(
                street_points, batch_start, batch_size, max_workers, db_service
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
            
            # Calculate processing rate
            points_per_second = len(batch_results['processed']) / batch_duration if batch_duration > 0 else 0
            
            logger.info(f"Batch {batch_num + 1}/{total_batches} completed in {batch_duration:.1f}s "
                       f"({points_per_second:.1f} points/sec)")
            logger.info(f"Progress: {progress_pct:.1f}% ({points_processed}/{total_points})")
            logger.info(f"Success: {overall_stats['successful']} points, {overall_stats['total_images']} images")
            logger.info(f"ETA: {eta_str}")
            logger.info("-" * 60)
        
        # Final results
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("="*80)
        logger.info("PARALLEL DATASET ANALYSIS COMPLETED")
        logger.info(f"Total duration: {duration}")
        logger.info("="*80)
        logger.info("FINAL STATISTICS:")
        logger.info(f"Street points processed: {overall_stats['processed']}")
        logger.info(f"Points with images found: {overall_stats['successful']}")
        logger.info(f"Points with no images: {overall_stats['no_images']}")
        logger.info(f"Processing failures: {overall_stats['failed']}")
        logger.info(f"Total images found: {overall_stats['total_images']}")
        logger.info(f"Total images saved to DB: {overall_stats['total_saved']}")
        
        if overall_stats['processed'] > 0:
            success_rate = (overall_stats['successful'] / overall_stats['processed']) * 100
            avg_time_per_point = duration.total_seconds() / overall_stats['processed']
            logger.info(f"Success rate: {success_rate:.3f}%")
            logger.info(f"Average time per point: {avg_time_per_point:.3f} seconds")
            
            if overall_stats['successful'] > 0:
                avg_images = overall_stats['total_images'] / overall_stats['successful']
                logger.info(f"Average images per successful point: {avg_images:.1f}")
                
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        logger.info(f"Processed {overall_stats.get('processed', 0)} points before interruption")
    except Exception as e:
        logger.error(f"Fatal error in pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()