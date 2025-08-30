"""
Database-integrated road analysis pipeline.

Extends the base pipeline to save all results to the database with:
- Duplicate detection
- Transaction safety
- Progress tracking
- Error recovery
"""

import logging
from datetime import datetime
from typing import Any, Optional

from ...database.services.database_service import DatabaseService
from .pipeline_result import PipelineResult
from .road_analysis_pipeline import RoadAnalysisPipeline


logger = logging.getLogger(__name__)


class DatabasePipeline(RoadAnalysisPipeline):
    """Road analysis pipeline with database integration."""

    def __init__(
        self, enable_fetcher: bool = False, database_service: Optional[DatabaseService] = None
    ) -> None:
        """
        Initialize database-integrated pipeline.

        Args:
            enable_fetcher: Enable image fetching capability
            database_service: Database service instance (creates new if None)
        """
        super().__init__(enable_fetcher=enable_fetcher)

        self.db_service = database_service or DatabaseService()
        self.save_to_db = True

        logger.info("Database pipeline initialized")

    def _parse_mapillary_date(self, captured_at: Any) -> Optional[datetime]:
        """Parse Mapillary date which can be ISO string or timestamp."""
        logger.debug(f"DEBUG: _parse_mapillary_date received: {captured_at} (type: {type(captured_at)})")
        
        if not captured_at:
            logger.debug(f"DEBUG: captured_at is falsy: {captured_at}")
            return None

        try:
            # Try parsing as ISO string first
            if isinstance(captured_at, str):
                result = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
                logger.debug(f"DEBUG: Parsed ISO string to: {result}")
                return result
            # Try parsing as timestamp
            if isinstance(captured_at, (int, float)):
                # Check if timestamp is in milliseconds (common for Mapillary)
                # Timestamps > 1e10 are likely milliseconds (after ~2001 in seconds)
                if captured_at > 1e10:
                    result = datetime.fromtimestamp(captured_at / 1000)
                    logger.debug(f"DEBUG: Parsed millisecond timestamp to: {result}")
                    return result
                else:
                    result = datetime.fromtimestamp(captured_at)
                    logger.debug(f"DEBUG: Parsed second timestamp to: {result}")
                    return result
            logger.debug(f"DEBUG: Unsupported type for captured_at: {type(captured_at)}")
            return None
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse Mapillary date: {captured_at}, error: {e}")
            return None

    def _validate_compass_angle(self, angle: Any) -> Optional[float]:
        """Validate compass angle to be between 0-360 degrees."""
        if angle is None:
            return None
        
        try:
            angle_float = float(angle)
            if 0 <= angle_float < 360:
                return angle_float
            else:
                logger.warning(f"Invalid compass angle {angle_float}, must be 0-360")
                return None
        except (ValueError, TypeError):
            logger.warning(f"Could not parse compass angle: {angle}")
            return None

    def process_image_with_db(
        self,
        image_path: str,
        source: str = "manual_upload",
        source_image_id: Optional[str] = None,
        location: Optional[tuple[float, float]] = None,
        date_taken: Optional[datetime] = None,
        compass_angle: Optional[float] = None,
        street_point_id: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Process image and save all results to database.

        Args:
            image_path: Path to image file
            source: Image source ('mapillary', 'streetview', 'manual_upload')
            source_image_id: Original platform image ID
            location: (latitude, longitude) tuple
            date_taken: When photo was captured
            compass_angle: Camera direction in degrees
            street_point_id: Reference to street point entry

        Returns:
            Dictionary with processing results and database IDs
        """
        logger.info(f"Processing image with database integration: {image_path}")

        try:
            # Check for duplicates first
            duplicate = self.db_service.check_duplicate_photo(
                source=source,
                source_image_id=source_image_id,
                location=location,
                date_taken=date_taken,
            )

            if duplicate:
                logger.info(
                    f"Found duplicate photo (ID: {duplicate['id']}), returning existing results"
                )
                existing_results = self.db_service.get_photo_with_results(duplicate["id"])
                return {
                    "duplicate_found": True,
                    "photo_id": duplicate["id"],
                    "existing_results": existing_results,
                    "processing_skipped": True,
                }

            # Process image with base pipeline
            pipeline_result = self.process_image(image_path)

            if not self.save_to_db:
                return {"pipeline_result": pipeline_result, "database_saved": False}

            # Save to database in transaction
            return self._save_pipeline_result_to_db(
                pipeline_result=pipeline_result,
                source=source,
                source_image_id=source_image_id,
                location=location,
                date_taken=date_taken,
                compass_angle=compass_angle,
                street_point_id=street_point_id,
            )

        except Exception as e:
            logger.error(f"Error processing image {image_path}: {e}")
            return {"error": str(e), "success": False, "image_path": image_path}

    def _save_pipeline_result_to_db(
        self,
        pipeline_result: PipelineResult,
        source: str,
        source_image_id: str = None,
        location: tuple[float, float] = None,
        date_taken: datetime = None,
        compass_angle: float = None,
        street_point_id: int = None,
    ) -> dict[str, Any]:
        """Save pipeline result to database with transaction safety."""

        with self.db_service.transaction():
            try:
                # 1. Save photo metadata
                photo_id = self.db_service.save_photo(
                    source=source,
                    source_image_id=source_image_id,
                    location=location,
                    date_taken=date_taken,
                    compass_angle=compass_angle,
                    street_point_id=street_point_id,  # Link to street point if available
                )

                # 2. Save quality assessment (always)
                quality_id = self.db_service.save_quality_result(
                    photo_id=photo_id, quality_metrics=pipeline_result.quality_metrics
                )

                # 3. Save road analysis (only if quality passed)
                road_analysis_id = None
                if pipeline_result.road_metrics:
                    road_analysis_id = self.db_service.save_road_analysis_result(
                        photo_id=photo_id, road_metrics=pipeline_result.road_metrics
                    )

                logger.info(f"Successfully saved pipeline result to database: photo_id={photo_id}")

                return {
                    "success": True,
                    "duplicate_found": False,
                    "pipeline_result": pipeline_result,
                    "database_ids": {
                        "photo_id": photo_id,
                        "quality_id": quality_id,
                        "road_analysis_id": road_analysis_id,
                    },
                    "database_saved": True,
                }

            except Exception as e:
                logger.error(f"Failed to save pipeline result to database: {e}")
                # Transaction will be rolled back automatically
                return {
                    "success": False,
                    "error": f"Database save failed: {e}",
                    "pipeline_result": pipeline_result,
                    "database_saved": False,
                }

    def process_coordinate_with_db(
        self,
        lat: float,
        lon: float,
        radius_m: float = 100.0,
        limit: int = 5,
        output_dir: str = None,
        street_point_id: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Process coordinate with image fetching and database integration.

        Args:
            lat: Latitude
            lon: Longitude
            radius_m: Search radius in meters
            limit: Maximum images to fetch
            output_dir: Directory to save images
            street_point_id: Optional street point ID to link photos to

        Returns:
            Processing results with database integration
        """
        logger.info(f"Processing coordinate with database: {lat:.4f}, {lon:.4f}")

        if not self.fetcher_service:
            raise ValueError("Fetcher service not enabled. Initialize with enable_fetcher=True")

        try:
            # Fetch images from coordinate
            fetch_result = self.fetcher_service.fetch_images_at_point(
                lat=lat, lon=lon, radius_m=radius_m, limit=limit, output_dir=output_dir
            )

            if not fetch_result["success"] or not fetch_result["image_paths"]:
                return {
                    "fetch_result": fetch_result,
                    "processed_images": [],
                    "database_results": [],
                    "summary": {
                        "total_images_fetched": 0,
                        "total_processed": 0,
                        "successful_database_saves": 0,
                        "duplicates_found": 0,
                        "processing_errors": 0,
                    },
                    "success": fetch_result["success"],
                }

            # Process each downloaded image with database integration
            processed_images = []
            database_results = []

            # Combine image paths with metadata
            image_paths = fetch_result["image_paths"]
            image_metadata_list = fetch_result["image_metadata"]

            for i, image_path in enumerate(image_paths):
                # Get corresponding metadata (if available)
                mapillary_data = image_metadata_list[i] if i < len(image_metadata_list) else {}
                
                # DEBUG: Log what mapillary_data contains
                logger.debug(f"DEBUG: Processing image {i}: {image_path}")
                logger.debug(f"DEBUG: mapillary_data keys: {list(mapillary_data.keys()) if mapillary_data else 'EMPTY'}")
                if mapillary_data:
                    logger.debug(f"DEBUG: captured_at value: {mapillary_data.get('captured_at')} (type: {type(mapillary_data.get('captured_at'))})")

                result = self.process_image_with_db(
                    image_path=image_path,
                    source="mapillary",
                    source_image_id=mapillary_data.get("id"),
                    location=(
                        mapillary_data.get("geometry", {}).get("coordinates", [None, None])[
                            1
                        ],  # lat
                        mapillary_data.get("geometry", {}).get("coordinates", [None, None])[
                            0
                        ],  # lon
                    )
                    if mapillary_data.get("geometry")
                    else None,
                    date_taken=self._parse_mapillary_date(mapillary_data.get("captured_at")),
                    compass_angle=self._validate_compass_angle(mapillary_data.get("compass_angle")),
                    street_point_id=street_point_id,
                )

                processed_images.append(result)
                if result.get("database_saved"):
                    database_results.append(result["database_ids"])

            # Calculate summary statistics
            total_processed = len(processed_images)
            successful_saves = len([r for r in processed_images if r.get("database_saved")])
            duplicates_found = len([r for r in processed_images if r.get("duplicate_found")])

            logger.info(
                f"Coordinate processing complete: {successful_saves}/{total_processed} saved to database, {duplicates_found} duplicates found"
            )

            return {
                "fetch_result": fetch_result,
                "processed_images": processed_images,
                "database_results": database_results,
                "summary": {
                    "total_images_fetched": len(fetch_result.get("image_paths", [])),
                    "total_processed": total_processed,
                    "successful_database_saves": successful_saves,
                    "duplicates_found": duplicates_found,
                    "processing_errors": total_processed - successful_saves - duplicates_found,
                },
                "success": True,
            }

        except Exception as e:
            logger.error(f"Error processing coordinate {lat}, {lon}: {e}")
            return {"error": str(e), "success": False, "coordinates": (lat, lon)}

    def process_all_street_points(
        self, 
        radius_m: float = 1, 
        images_per_point: int = 20,
        batch_size: int = 100,
        start_offset: int = 0
    ) -> dict[str, Any]:
        """
        Process all street points in the database with image analysis.
        
        Args:
            radius_m: Search radius in meters for images around each point
            images_per_point: Maximum images to fetch per street point
            batch_size: Number of street points to process in each batch
            start_offset: Offset to start processing from (for resuming)
            
        Returns:
            Processing summary with statistics
        """
        import time
        
        if not self.fetcher_service:
            raise ValueError("Fetcher service not enabled. Initialize with enable_fetcher=True")
            
        logger.info(f"Starting full street points analysis with {radius_m}m radius, {images_per_point} images per point")
        
        # Get total count
        with self.db_service.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) as total FROM street_points')
            total_count = cursor.fetchone()['total']
        
        logger.info(f'Processing {total_count:,} street points in batches of {batch_size}')
        start_time = time.time()
        
        # Initialize counters
        processed_points = 0
        total_images_found = 0
        total_images_saved = 0
        total_duplicates = 0
        total_errors = 0
        
        # Process in batches
        for offset in range(start_offset, total_count, batch_size):
            batch_start_time = time.time()
            
            # Get batch of street points
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, ST_X(location) as latitude, ST_Y(location) as longitude 
                    FROM street_points 
                    ORDER BY id 
                    LIMIT %s OFFSET %s
                ''', (batch_size, offset))
                
                batch_points = cursor.fetchall()
            
            # Process each point in the batch
            for point in batch_points:
                street_point_id = point['id']
                lon, lat = point['longitude'], point['latitude']
                
                try:
                    result = self.process_coordinate_with_db(
                        lat=lat, 
                        lon=lon, 
                        radius_m=radius_m, 
                        limit=images_per_point, 
                        street_point_id=street_point_id
                    )
                    
                    # Update counters
                    total_images_found += result['fetch_result']['images_found']
                    total_images_saved += result['summary']['successful_database_saves']
                    total_duplicates += result['summary']['duplicates_found']
                    processed_points += 1
                    
                except Exception as e:
                    logger.error(f"Error processing street point {street_point_id}: {e}")
                    total_errors += 1
                    continue
            
            # Progress reporting
            batch_time = time.time() - batch_start_time
            elapsed_total = time.time() - start_time
            
            if processed_points > 0:
                rate_per_min = processed_points / elapsed_total * 60
                eta_minutes = (total_count - processed_points - start_offset) / rate_per_min if rate_per_min > 0 else 0
                
                logger.info(
                    f"Batch complete: {processed_points + start_offset:,}/{total_count:,} "
                    f"({(processed_points + start_offset)/total_count*100:.1f}%) - "
                    f"Images found: {total_images_found:,}, Saved: {total_images_saved:,}, "
                    f"Duplicates: {total_duplicates:,}, Errors: {total_errors:,} - "
                    f"Rate: {rate_per_min:.1f}/min, ETA: {eta_minutes:.0f}min"
                )
        
        # Final summary
        total_time = time.time() - start_time
        logger.info(
            f"Street points analysis complete! Processed {processed_points:,} points in {total_time/60:.1f} minutes. "
            f"Found {total_images_found:,} images, saved {total_images_saved:,} to database."
        )
        
        return {
            "total_points_processed": processed_points,
            "total_images_found": total_images_found,
            "total_images_saved": total_images_saved,
            "total_duplicates": total_duplicates,
            "total_errors": total_errors,
            "processing_time_minutes": total_time / 60,
            "average_rate_per_minute": processed_points / (total_time / 60) if total_time > 0 else 0,
            "parameters": {
                "radius_m": radius_m,
                "images_per_point": images_per_point,
                "batch_size": batch_size,
                "start_offset": start_offset
            }
        }

    def get_database_stats(self) -> dict[str, Any]:
        """Get database processing statistics."""
        return self.db_service.get_processing_stats()

    def disable_database_saves(self):
        """Disable database saves (for testing/debugging)."""
        self.save_to_db = False
        logger.info("Database saves disabled")

    def enable_database_saves(self):
        """Re-enable database saves."""
        self.save_to_db = True
        logger.info("Database saves enabled")
