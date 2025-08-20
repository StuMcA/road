"""
Street Pipeline Service

Implements the streamlined 3-step process:
1. Features API: Fetch TOIDs and coordinates
2. Names API: Get street/location metadata for each coordinate
3. Database: Save combined data
"""

import logging
from typing import List, Dict, Any, Optional

from ....database.services.database_service import DatabaseService
from ...toid_metadata_service import TOIDMetadataService


logger = logging.getLogger(__name__)


class StreetPipelineService:
    """Service for running streamlined street data pipeline."""
    
    def __init__(self):
        self.toid_metadata_service = TOIDMetadataService()
    
    def run_street_pipeline(
        self, 
        bbox: List[float], 
        max_toids: Optional[int] = None
    ) -> Dict[str, Any]:
        """Run the streamlined street data pipeline.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            max_toids: Optional limit on number of TOIDs to process
            
        Returns:
            Pipeline results with counts and statistics
        """
        logger.info(f"Running streamlined street pipeline for bbox: {bbox}")
        
        # Delegate to the TOID metadata service which handles the 3-step process
        results = self.toid_metadata_service.process_area(bbox, max_toids)
        
        # Rename fields to match expected interface
        pipeline_results = {
            "success": results["success"],
            "bbox": results["bbox"],
            "toids_fetched": results["toids_fetched"],
            "toids_with_metadata": results["toids_with_metadata"],
            "toids_saved": results["toids_saved"],
            "errors": results["errors"]
        }
        
        return pipeline_results
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get information about the street pipeline service."""
        return {
            "service_name": "StreetPipelineService", 
            "description": "Streamlined pipeline using TOIDMetadataService",
            "components": {
                "toid_metadata_service": self.toid_metadata_service.get_service_info()
            }
        }
    