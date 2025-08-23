"""
TOID-based Road Analysis Pipeline

This pipeline is purely database-driven and focuses on image analysis:
1. Reads existing street data from database (no client calls)
2. Creates bounding boxes around TOID coordinates  
3. Fetches Mapillary photos for each bbox
4. Performs quality and road analysis on photos
5. Saves all results to database

Note: Street data must be pre-populated in database using StreetDataService
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ...database.services.database_service import DatabaseService
from ...utils.coord_utils import bbox_from_point
from .database_pipeline import DatabasePipeline


logger = logging.getLogger(__name__)


class TOIDAnalysisPipeline:
    """
    Database-driven TOID-based road analysis pipeline.
    
    This pipeline only reads from the database and does not make any external API calls.
    Street data must be pre-populated using StreetDataService.
    """
    
    def __init__(self, 
                 bbox_radius_m: float = 50.0,
                 photos_per_bbox: int = 5,
                 enable_database: bool = True,
                 database_service: Optional[DatabaseService] = None):
        """
        Initialize the TOID analysis pipeline.
        
        Args:
            bbox_radius_m: Radius in meters for bbox creation around each TOID
            photos_per_bbox: Maximum photos to fetch per TOID location
            enable_database: Whether to save results to database
            database_service: Optional database service instance (creates new if None)
        """
        self.bbox_radius_m = bbox_radius_m
        self.photos_per_bbox = photos_per_bbox
        self.enable_database = enable_database
        
        # Initialize database-only services (no external API clients)
        self.db_service = database_service or DatabaseService()
        self.database_pipeline = DatabasePipeline(
            enable_fetcher=True, 
            database_service=self.db_service
        ) if enable_database else None
        
        self.version = "3.0.0"  # Database-only version
        
        # Session statistics
        self.session_stats = {
            "start_time": None,
            "end_time": None,
            "toids_fetched": 0,
            "bboxes_created": 0,
            "photos_found": 0,
            "photos_processed": 0,
            "photos_saved": 0,
            "processing_errors": 0,
            "total_processing_time_ms": 0
        }
        
        logger.info("TOID Analysis Pipeline initialized")
    
    def fetch_street_data_for_area(self, 
                                 area_bbox: List[float], 
                                 max_features: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch street data (TOIDs with metadata) from database for a given area.
        
        This method only reads from the database and will not make external API calls.
        If no data exists, it fails gracefully.
        
        Args:
            area_bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
            max_features: Maximum number of features to retrieve
            
        Returns:
            List of street data dictionaries from database
            
        Raises:
            ValueError: If no street data found in database for the area
        """
        logger.info(f"Querying street data from database for area: {area_bbox}")
        
        try:
            # Query street_data table directly
            street_data = self._query_street_data_from_db(
                bbox=area_bbox,
                limit=max_features
            )
            
            if not street_data:
                error_msg = (
                    f"No street data found in database for area {area_bbox}. "
                    "Please run StreetDataService.collect_street_data() first to populate the database."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            self.session_stats["toids_fetched"] = len(street_data)
            logger.info(f"Retrieved {len(street_data)} street data records from database")
            
            return street_data
            
        except Exception as e:
            if isinstance(e, ValueError):
                # Re-raise ValueError as-is (graceful failure)
                raise
            else:
                # Log and re-raise other exceptions
                logger.error(f"Database error fetching street data: {e}")
                self.session_stats["processing_errors"] += 1
                raise RuntimeError(f"Database query failed: {e}") from e
    
    def _query_street_data_from_db(
        self, 
        bbox: List[float], 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query street data directly from database.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            limit: Optional limit on results
            
        Returns:
            List of street data dictionaries
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                query = """
                    SELECT 
                        sp.id, sp.toid, sp.version_date, sp.source_product,
                        ST_X(sp.location) as longitude, ST_Y(sp.location) as latitude, 
                        sp.easting, sp.northing,
                        s.street_name, sp.local_authority as locality, sp.region, sp.postcode as postcode_area,
                        sp.created_at, sp.created_at as updated_at
                    FROM street_points sp
                    LEFT JOIN streets s ON sp.street_id = s.id
                    WHERE sp.toid IS NOT NULL
                      AND ST_Within(
                        sp.location,
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                    )
                    ORDER BY sp.created_at DESC
                """
                
                params = bbox
                if limit:
                    query += f" LIMIT {limit}"
                    
                cursor.execute(query, params)
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to query street data from database: {e}")
            raise
    
    def create_bbox_from_toid(self, toid_info: Dict[str, Any]) -> List[float]:
        """
        Create a bounding box around a TOID's coordinates.
        
        Args:
            toid_info: TOID information dictionary
            
        Returns:
            Bounding box as [min_lon, min_lat, max_lon, max_lat]
        """
        lat = toid_info["latitude"]
        lon = toid_info["longitude"]
        
        # Create bbox using utility function
        min_lat, min_lon, max_lat, max_lon = bbox_from_point(
            lat, lon, self.bbox_radius_m
        )
        
        return [min_lon, min_lat, max_lon, max_lat]
    
    def process_toid_batch(self, 
                          street_data: List[Dict[str, Any]], 
                          output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a batch of street data through the complete image analysis pipeline.
        
        Args:
            street_data: List of street data dictionaries with TOID and metadata
            output_dir: Optional directory for saving images
            
        Returns:
            Complete processing results
        """
        start_time = time.time()
        self.session_stats["start_time"] = datetime.now()
        
        logger.info(f"Processing batch of {len(street_data)} street data entries")
        
        results = {
            "session_metadata": {
                "pipeline_version": self.version,
                "start_time": self.session_stats["start_time"].isoformat(),
                "bbox_radius_m": self.bbox_radius_m,
                "photos_per_bbox": self.photos_per_bbox,
                "total_street_data_entries": len(street_data)
            },
            "toid_results": [],
            "session_summary": {},
            "processing_errors": []
        }
        
        for i, street_entry in enumerate(street_data, 1):
            logger.info(f"Processing street data {i}/{len(street_data)}: {street_entry['toid']}")
            
            try:
                # Step 1: Create bbox around location
                bbox = self.create_bbox_from_toid(street_entry)
                self.session_stats["bboxes_created"] += 1
                
                # Step 2: Process bbox with database pipeline, linking to street data
                if self.database_pipeline:
                    toid_output_dir = f"{output_dir}/toid_{street_entry['toid']}" if output_dir else None
                    
                    # Modify the database pipeline to link photos to street data
                    pipeline_result = self._process_coordinate_with_street_data(
                        street_entry=street_entry,
                        radius_m=self.bbox_radius_m,
                        limit=self.photos_per_bbox,
                        output_dir=toid_output_dir
                    )
                    
                    # Combine street data with pipeline results
                    toid_result = {
                        "street_data": street_entry,
                        "bbox": bbox,
                        "pipeline_result": pipeline_result
                    }
                    
                    # Update session stats
                    if pipeline_result.get("success"):
                        summary = pipeline_result.get("summary", {})
                        self.session_stats["photos_found"] += summary.get("total_images_fetched", 0)
                        self.session_stats["photos_processed"] += summary.get("total_processed", 0)
                        self.session_stats["photos_saved"] += summary.get("successful_database_saves", 0)
                    
                else:
                    # No database pipeline - just create bbox info
                    toid_result = {
                        "street_data": street_entry,
                        "bbox": bbox,
                        "pipeline_result": {"note": "Database pipeline disabled"}
                    }
                
                results["toid_results"].append(toid_result)
                
                # Log progress every 10 entries
                if i % 10 == 0 or i == len(street_data):
                    logger.info(f"Progress: {i}/{len(street_data)} street data entries processed")
                
            except Exception as e:
                error_info = {
                    "toid": street_entry["toid"],
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                results["processing_errors"].append(error_info)
                self.session_stats["processing_errors"] += 1
                logger.error(f"Error processing street data entry {street_entry['toid']}: {e}")
                continue
        
        # Finalize session
        self.session_stats["end_time"] = datetime.now()
        self.session_stats["total_processing_time_ms"] = (time.time() - start_time) * 1000
        
        # Create session summary
        results["session_summary"] = self.session_stats.copy()
        results["session_summary"]["start_time"] = self.session_stats["start_time"].isoformat()
        results["session_summary"]["end_time"] = self.session_stats["end_time"].isoformat()
        results["session_summary"]["success_rate"] = (
            (len(street_data) - self.session_stats["processing_errors"]) / len(street_data) * 100
            if len(street_data) > 0 else 0
        )
        
        logger.info(f"Batch processing complete: {len(results['toid_results'])} successful, "
                   f"{len(results['processing_errors'])} errors")
        
        return results
    
    def _process_coordinate_with_street_data(
        self,
        street_entry: Dict[str, Any],
        radius_m: float = 100.0,
        limit: int = 5,
        output_dir: str = None,
    ) -> dict[str, Any]:
        """
        Process coordinate with image fetching and database integration, 
        linking photos to street data entry.

        Args:
            street_entry: Street data dictionary from StreetDataService
            radius_m: Search radius in meters
            limit: Maximum images to fetch
            output_dir: Directory to save images

        Returns:
            Processing results with database integration and street data linking
        """
        lat = street_entry["latitude"]
        lon = street_entry["longitude"]
        street_data_id = street_entry["id"]
        
        logger.info(f"Processing coordinate {lat:.4f}, {lon:.4f} with street_data_id {street_data_id}")

        if not self.database_pipeline.fetcher_service:
            raise ValueError("Fetcher service not enabled. Initialize with enable_fetcher=True")

        try:
            # Fetch images from coordinate
            fetch_result = self.database_pipeline.fetcher_service.fetch_images_at_point(
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
                    "street_data_linked": True
                }

            # Process each downloaded image with database integration AND street data linking
            processed_images = []
            database_results = []

            # Combine image paths with metadata
            image_paths = fetch_result["image_paths"]
            image_metadata_list = fetch_result["image_metadata"]

            for i, image_path in enumerate(image_paths):
                # Get corresponding metadata (if available)
                mapillary_data = image_metadata_list[i] if i < len(image_metadata_list) else {}

                result = self.database_pipeline.process_image_with_db(
                    image_path=image_path,
                    source="mapillary",
                    source_image_id=mapillary_data.get("id"),
                    location=(
                        mapillary_data.get("geometry", {}).get("coordinates", [None, None])[1],  # lat
                        mapillary_data.get("geometry", {}).get("coordinates", [None, None])[0],  # lon
                    ) if mapillary_data.get("geometry") else None,
                    date_taken=self.database_pipeline._parse_mapillary_date(mapillary_data.get("captured_at")),
                    compass_angle=self.database_pipeline._validate_compass_angle(mapillary_data.get("compass_angle")),
                    street_point_id=street_data_id  # KEY: Link to street point
                )

                processed_images.append(result)
                if result.get("database_saved"):
                    database_results.append(result["database_ids"])

            # Calculate summary statistics
            total_processed = len(processed_images)
            successful_saves = len([r for r in processed_images if r.get("database_saved")])
            duplicates_found = len([r for r in processed_images if r.get("duplicate_found")])

            logger.info(
                f"Coordinate processing complete: {successful_saves}/{total_processed} saved to database, "
                f"{duplicates_found} duplicates found, all linked to street_data_id {street_data_id}"
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
                "street_data_linked": True,
                "street_data_id": street_data_id
            }

        except Exception as e:
            logger.error(f"Error processing coordinate {lat}, {lon} with street data: {e}")
            return {
                "error": str(e), 
                "success": False, 
                "coordinates": (lat, lon),
                "street_data_id": street_data_id,
                "street_data_linked": False
            }
    
    def run_area_analysis(self, 
                         area_bbox: List[float],
                         max_features: int = 100,
                         output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Run complete analysis for an area.
        
        This is the main entry point that:
        1. Fetches street data (TOIDs + metadata) from StreetDataService
        2. Processes each location through image analysis pipeline
        3. Returns comprehensive results
        
        Args:
            area_bbox: Area bounding box [min_lon, min_lat, max_lon, max_lat]
            max_features: Maximum features to process
            output_dir: Optional output directory for images
            
        Returns:
            Complete analysis results
        """
        logger.info(f"Starting area analysis: {area_bbox}, max_features={max_features}")
        
        try:
            # Step 1: Fetch street data from database only
            street_data = self.fetch_street_data_for_area(area_bbox, max_features)
            
            # Step 2: Process street data through image analysis
            results = self.process_toid_batch(street_data, output_dir)
            results["success"] = True
            results["area_bbox"] = area_bbox
            
            return results
            
        except ValueError as e:
            # Graceful failure for missing data
            logger.warning(f"Street data not available for area: {e}")
            return {
                "success": False,
                "error": str(e),
                "error_type": "missing_data",
                "area_bbox": area_bbox,
                "session_stats": self.session_stats,
                "recommendation": "Run StreetDataService.collect_street_data() to populate the database first"
            }
        except Exception as e:
            # Other failures (database errors, etc.)
            logger.error(f"Area analysis failed with unexpected error: {e}")
            return {
                "success": False,
                "error": str(e),
                "error_type": "system_error",
                "area_bbox": area_bbox,
                "session_stats": self.session_stats
            }
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """Get information about the pipeline configuration."""
        return {
            "pipeline_name": "TOIDAnalysisPipeline",
            "version": self.version,
            "mode": "database_only",
            "description": "Database-driven pipeline that processes street data without external API calls",
            "configuration": {
                "bbox_radius_m": self.bbox_radius_m,
                "photos_per_bbox": self.photos_per_bbox,
                "database_enabled": self.enable_database
            },
            "dependencies": {
                "database_service": "DatabaseService",
                "database_pipeline": (
                    self.database_pipeline.get_service_info() 
                    if self.database_pipeline else None
                )
            },
            "requirements": [
                "Street data must be pre-populated in database using StreetDataService",
                "Database connection must be available",
                "Image fetching capabilities enabled via DatabasePipeline"
            ]
        }
    
    def check_data_availability(self, area_bbox: List[float]) -> Dict[str, Any]:
        """
        Check if street data is available in the database for a given area.
        
        Args:
            area_bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
            
        Returns:
            Dictionary with availability status and count
        """
        try:
            street_data = self._query_street_data_from_db(bbox=area_bbox, limit=None)
            
            return {
                "data_available": len(street_data) > 0,
                "count": len(street_data),
                "area_bbox": area_bbox,
                "message": f"Found {len(street_data)} street data entries" if street_data else "No street data found"
            }
            
        except Exception as e:
            logger.error(f"Error checking data availability: {e}")
            return {
                "data_available": False,
                "count": 0,
                "area_bbox": area_bbox,
                "error": str(e),
                "message": "Error checking database"
            }
    
    def get_database_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about street data in the database.
        
        Returns:
            Dictionary with database statistics
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                # Get basic counts
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_street_data,
                        COUNT(s.street_name) as entries_with_street_name,
                        COUNT(DISTINCT s.street_name) as unique_street_names,
                        COUNT(DISTINCT sp.local_authority) as unique_localities,
                        MIN(sp.created_at) as earliest_entry,
                        MAX(sp.created_at) as latest_entry
                    FROM street_points sp
                    LEFT JOIN streets s ON sp.street_id = s.id
                    WHERE sp.toid IS NOT NULL
                """)
                
                stats = dict(cursor.fetchone())
                
                # Get geographical bounds
                cursor.execute("""
                    SELECT 
                        ST_XMin(ST_Extent(location)) as min_longitude,
                        ST_YMin(ST_Extent(location)) as min_latitude,
                        ST_XMax(ST_Extent(location)) as max_longitude,
                        ST_YMax(ST_Extent(location)) as max_latitude
                    FROM street_points
                    WHERE location IS NOT NULL AND toid IS NOT NULL
                """)
                
                bounds_result = cursor.fetchone()
                if bounds_result:
                    bounds = dict(bounds_result)
                    stats["geographical_bounds"] = bounds
                
                return {
                    "success": True,
                    "statistics": stats
                }
                
        except Exception as e:
            logger.error(f"Error getting database statistics: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    