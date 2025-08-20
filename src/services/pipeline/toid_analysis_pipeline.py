"""
TOID-based Road Analysis Pipeline

This overarching pipeline:
1. Fetches TOIDs from OS Features API
2. Creates bboxes around TOID coordinates  
3. Fetches Mapillary photos for each bbox
4. Performs quality and road analysis on photos
5. Saves all results to database
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ...clients.ordnance_survey.os_features_client import OSFeaturesClient
from ...database.services.database_service import DatabaseService
from ...utils.coord_utils import bbox_from_point
from .database_pipeline import DatabasePipeline


logger = logging.getLogger(__name__)


class TOIDAnalysisPipeline:
    """
    Complete TOID-based road analysis pipeline.
    
    Orchestrates the entire process from TOID fetching to road quality analysis.
    """
    
    def __init__(self, 
                 bbox_radius_m: float = 50.0,
                 photos_per_bbox: int = 5,
                 enable_database: bool = True):
        """
        Initialize the TOID analysis pipeline.
        
        Args:
            bbox_radius_m: Radius in meters for bbox creation around each TOID
            photos_per_bbox: Maximum photos to fetch per TOID location
            enable_database: Whether to save results to database
        """
        self.bbox_radius_m = bbox_radius_m
        self.photos_per_bbox = photos_per_bbox
        self.enable_database = enable_database
        
        # Initialize services
        self.os_client = OSFeaturesClient()
        self.database_pipeline = DatabasePipeline(enable_fetcher=True) if enable_database else None
        self.db_service = DatabaseService() if enable_database else None
        
        self.version = "1.0.0"
        
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
    
    def fetch_toids_for_area(self, 
                           area_bbox: List[float], 
                           max_toids: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch TOIDs from OS Features API for a given area.
        
        Args:
            area_bbox: Bounding box [min_lon, min_lat, max_lon, max_lat]
            max_toids: Maximum number of TOIDs to fetch
            
        Returns:
            List of TOID data dictionaries
        """
        logger.info(f"Fetching up to {max_toids} TOIDs from area: {area_bbox}")
        
        try:
            # Fetch TOIDs from OS API
            toid_features = self.os_client.fetch_all_toid_features(
                bbox=area_bbox,
                count=100,  # Fetch in batches of 100
                max_features=max_toids
            )
            
            # Convert to our format
            toids = []
            for toid_data in toid_features:
                toid, version_date, source_product, geom_wkt, longitude, latitude = toid_data
                
                toid_info = {
                    "toid": toid,
                    "longitude": longitude,
                    "latitude": latitude,
                    "version_date": version_date.isoformat() if version_date else None,
                    "source_product": source_product,
                    "geometry_wkt": geom_wkt
                }
                toids.append(toid_info)
            
            self.session_stats["toids_fetched"] = len(toids)
            logger.info(f"Successfully fetched {len(toids)} TOIDs")
            
            return toids
            
        except Exception as e:
            logger.error(f"Error fetching TOIDs: {e}")
            self.session_stats["processing_errors"] += 1
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
                          toids: List[Dict[str, Any]], 
                          output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a batch of TOIDs through the complete pipeline.
        
        Args:
            toids: List of TOID information dictionaries
            output_dir: Optional directory for saving images
            
        Returns:
            Complete processing results
        """
        start_time = time.time()
        self.session_stats["start_time"] = datetime.now()
        
        logger.info(f"Processing batch of {len(toids)} TOIDs")
        
        results = {
            "session_metadata": {
                "pipeline_version": self.version,
                "start_time": self.session_stats["start_time"].isoformat(),
                "bbox_radius_m": self.bbox_radius_m,
                "photos_per_bbox": self.photos_per_bbox,
                "total_toids": len(toids)
            },
            "toid_results": [],
            "session_summary": {},
            "processing_errors": []
        }
        
        for i, toid_info in enumerate(toids, 1):
            logger.info(f"Processing TOID {i}/{len(toids)}: {toid_info['toid']}")
            
            try:
                # Step 1: Create bbox around TOID
                bbox = self.create_bbox_from_toid(toid_info)
                self.session_stats["bboxes_created"] += 1
                
                # Step 2: Process bbox with database pipeline
                if self.database_pipeline:
                    toid_output_dir = f"{output_dir}/toid_{toid_info['toid']}" if output_dir else None
                    
                    pipeline_result = self.database_pipeline.process_coordinate_with_db(
                        lat=toid_info["latitude"],
                        lon=toid_info["longitude"], 
                        radius_m=self.bbox_radius_m,
                        limit=self.photos_per_bbox,
                        output_dir=toid_output_dir
                    )
                    
                    # Combine TOID info with pipeline results
                    toid_result = {
                        "toid_info": toid_info,
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
                        "toid_info": toid_info,
                        "bbox": bbox,
                        "pipeline_result": {"note": "Database pipeline disabled"}
                    }
                
                results["toid_results"].append(toid_result)
                
                # Log progress every 10 TOIDs
                if i % 10 == 0 or i == len(toids):
                    logger.info(f"Progress: {i}/{len(toids)} TOIDs processed")
                
            except Exception as e:
                error_info = {
                    "toid": toid_info["toid"],
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                results["processing_errors"].append(error_info)
                self.session_stats["processing_errors"] += 1
                logger.error(f"Error processing TOID {toid_info['toid']}: {e}")
                continue
        
        # Finalize session
        self.session_stats["end_time"] = datetime.now()
        self.session_stats["total_processing_time_ms"] = (time.time() - start_time) * 1000
        
        # Create session summary
        results["session_summary"] = self.session_stats.copy()
        results["session_summary"]["start_time"] = self.session_stats["start_time"].isoformat()
        results["session_summary"]["end_time"] = self.session_stats["end_time"].isoformat()
        results["session_summary"]["success_rate"] = (
            (len(toids) - self.session_stats["processing_errors"]) / len(toids) * 100
            if len(toids) > 0 else 0
        )
        
        logger.info(f"Batch processing complete: {len(results['toid_results'])} successful, "
                   f"{len(results['processing_errors'])} errors")
        
        return results
    
    def run_area_analysis(self, 
                         area_bbox: List[float],
                         max_toids: int = 100,
                         output_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        Run complete analysis for an area.
        
        This is the main entry point that:
        1. Fetches TOIDs from the area
        2. Processes each TOID through the pipeline
        3. Returns comprehensive results
        
        Args:
            area_bbox: Area bounding box [min_lon, min_lat, max_lon, max_lat]
            max_toids: Maximum TOIDs to process
            output_dir: Optional output directory for images
            
        Returns:
            Complete analysis results
        """
        logger.info(f"Starting area analysis: {area_bbox}, max_toids={max_toids}")
        
        try:
            # Step 1: Fetch TOIDs
            toids = self.fetch_toids_for_area(area_bbox, max_toids)
            
            if not toids:
                logger.warning("No TOIDs found in area")
                return {
                    "success": False,
                    "error": "No TOIDs found in specified area",
                    "area_bbox": area_bbox
                }
            
            # Step 2: Process TOIDs
            results = self.process_toid_batch(toids, output_dir)
            results["success"] = True
            results["area_bbox"] = area_bbox
            
            return results
            
        except Exception as e:
            logger.error(f"Area analysis failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "area_bbox": area_bbox,
                "session_stats": self.session_stats
            }
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """Get information about the pipeline configuration."""
        return {
            "pipeline_name": "TOIDAnalysisPipeline",
            "version": self.version,
            "configuration": {
                "bbox_radius_m": self.bbox_radius_m,
                "photos_per_bbox": self.photos_per_bbox,
                "database_enabled": self.enable_database
            },
            "services": {
                "os_client": self.os_client.get_service_info() if self.os_client else None,
                "database_pipeline": (
                    self.database_pipeline.get_service_info() 
                    if self.database_pipeline else None
                )
            }
        }