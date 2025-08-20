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
        if not captured_at:
            return None

        try:
            # Try parsing as ISO string first
            if isinstance(captured_at, str):
                return datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
            # Try parsing as timestamp
            if isinstance(captured_at, (int, float)):
                return datetime.fromtimestamp(captured_at)
            return None
        except (ValueError, TypeError):
            logger.warning(f"Could not parse Mapillary date: {captured_at}")
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
        street_data_id: Optional[int] = None,
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
            street_data_id: Reference to street data entry (TOID-based)

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
                street_data_id=street_data_id,
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
        street_data_id: int = None,
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
                    street_point_id=None,  # Phase 1: no street points yet
                    street_data_id=street_data_id,  # Link to street data if available
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
    ) -> dict[str, Any]:
        """
        Process coordinate with image fetching and database integration.

        Args:
            lat: Latitude
            lon: Longitude
            radius_m: Search radius in meters
            limit: Maximum images to fetch
            output_dir: Directory to save images

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
