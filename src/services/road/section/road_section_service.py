"""
Simple Road Section Service

Fetches road sections from OS Features API and saves them to database.
"""

import logging
from typing import List, Optional

from ....database.services.database_service import DatabaseService
from ....clients.ordnance_survey.os_features_client import OSFeaturesClient


logger = logging.getLogger(__name__)


class RoadSectionService:
    """Simple service for fetching road sections and saving to database."""
    
    def __init__(self):
        self.os_client = OSFeaturesClient()
        self.db_service = DatabaseService()
    
    def fetch_and_save_road_sections(self, bbox: List[float]) -> dict:
        """Fetch road sections from OS API and save to database.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            
        Returns:
            Result dictionary with success status and counts
        """
        try:
            logger.info(f"Fetching road sections for bbox: {bbox}")
            
            # Fetch from OS API
            features = self.os_client.fetch_all_toid_features(bbox)
            
            # Save to database
            saved_count = 0
            for feature_data in features:
                toid, version_date, source_product, geom_wkt, longitude, latitude = feature_data
                
                # Save to toid_points table
                try:
                    with self.db_service.transaction() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            INSERT INTO toid_points (toid, version_date, source_product, geom, longitude, latitude)
                            VALUES (%s, %s, %s, ST_GeomFromText(%s), %s, %s)
                            ON CONFLICT (toid) DO NOTHING
                            """,
                            (toid, version_date, source_product, geom_wkt, longitude, latitude)
                        )
                        if cursor.rowcount > 0:
                            saved_count += 1
                except Exception as e:
                    logger.warning(f"Failed to save TOID {toid}: {e}")
                    continue
            
            logger.info(f"Saved {saved_count} new road sections from {len(features)} total")
            
            return {
                "success": True,
                "total_fetched": len(features),
                "saved_count": saved_count,
                "duplicates_skipped": len(features) - saved_count
            }
            
        except Exception as e:
            logger.error(f"Error in fetch_and_save_road_sections: {e}")
            return {
                "success": False,
                "error": str(e),
                "total_fetched": 0,
                "saved_count": 0
            }
        