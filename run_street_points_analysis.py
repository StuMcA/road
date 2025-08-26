#!/usr/bin/env python3
"""
Script to run process_all_street_points for all points in the database.
"""

import logging
import sys
from src.services.pipeline.database_pipeline import DatabasePipeline
from src.database.services.database_service import DatabaseService


def setup_logging():
    """Configure logging for the analysis."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('street_points_analysis.log')
        ]
    )


def main():
    """Run analysis on all street points."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize pipeline with fetcher enabled
        pipeline = DatabasePipeline(enable_fetcher=True)
        
        # Check current database stats before starting
        db_stats = pipeline.get_database_stats()
        logger.info(f"Database stats before processing: {db_stats}")
        
        # Run analysis on all street points
        # Using smaller radius (8m) and 5 images per point for better coverage
        logger.info("Starting analysis of all street points...")
        
        result = pipeline.process_all_street_points(
            radius_m=8.0,
            images_per_point=5,
            batch_size=100,
            start_offset=0
        )
        
        # Print final results
        logger.info("="*80)
        logger.info("FINAL RESULTS:")
        logger.info(f"Points processed: {result['total_points_processed']:,}")
        logger.info(f"Images found: {result['total_images_found']:,}")
        logger.info(f"Images saved: {result['total_images_saved']:,}")
        logger.info(f"Duplicates: {result['total_duplicates']:,}")
        logger.info(f"Errors: {result['total_errors']:,}")
        logger.info(f"Processing time: {result['processing_time_minutes']:.1f} minutes")
        logger.info(f"Average rate: {result['average_rate_per_minute']:.1f} points/minute")
        logger.info("="*80)
        
        # Check database stats after processing
        final_stats = pipeline.get_database_stats()
        logger.info(f"Database stats after processing: {final_stats}")
        
        print(f"\nProcessing complete! Processed {result['total_points_processed']:,} points")
        print(f"Found {result['total_images_found']:,} images, saved {result['total_images_saved']:,} to database")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()