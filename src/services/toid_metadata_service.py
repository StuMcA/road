"""
TOID Metadata Service

Implements the streamlined 3-step process:
1. Features API: Fetch TOIDs and coordinates
2. Names API: Get street/location metadata for each coordinate  
3. Database: Save combined data
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from clients.ordnance_survey.os_features_client import OSFeaturesClient
from clients.ordnance_survey.os_names_client import OSNamesClient
from database.services.database_service import DatabaseService


logger = logging.getLogger(__name__)


class TOIDMetadataService:
    """Service for fetching TOIDs with metadata and storing in database."""
    
    def __init__(self):
        self.features_client = OSFeaturesClient()
        self.names_client = OSNamesClient()
        self.db_service = DatabaseService()
    
    def process_area(
        self, 
        bbox: List[float], 
        max_toids: Optional[int] = None
    ) -> Dict[str, Any]:
        """Process an area following the 3-step workflow.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            max_toids: Optional limit on number of TOIDs to process
            
        Returns:
            Processing results with counts and statistics
        """
        results = {
            "success": False,
            "bbox": bbox,
            "toids_fetched": 0,
            "toids_with_metadata": 0,
            "toids_saved": 0,
            "errors": []
        }
        
        try:
            logger.info(f"Starting TOID metadata processing for bbox: {bbox}")
            
            # Step 1: Fetch TOIDs and coordinates from Features API
            logger.info("Step 1: Fetching TOIDs from Features API...")
            toid_features = self.features_client.fetch_all_toid_features(
                bbox=bbox,
                max_features=max_toids
            )
            results["toids_fetched"] = len(toid_features)
            logger.info(f"Fetched {len(toid_features)} TOID features")
            
            if not toid_features:
                logger.warning("No TOID features found in specified bbox")
                results["success"] = True
                return results
            
            # Step 2: For each TOID coordinate, get street metadata from Names API
            logger.info("Step 2: Getting street metadata from Names API...")
            toids_with_metadata = []
            
            for i, toid_feature in enumerate(toid_features):
                toid, version_date, source_product, geom_wkt, longitude, latitude, easting, northing = toid_feature
                
                logger.debug(f"Processing TOID {i+1}/{len(toid_features)}: {toid}")
                
                try:
                    # Get street metadata using BNG coordinates
                    street_metadata = self.names_client.get_street_metadata(
                        easting=easting,
                        northing=northing,
                        radius=100  # 100m search radius
                    )
                    
                    if any(street_metadata.values()):
                        results["toids_with_metadata"] += 1
                        logger.debug(f"Found metadata for TOID {toid}")
                    else:
                        logger.debug(f"No metadata found for TOID {toid}")
                    
                    # Combine TOID data with street metadata
                    combined_data = {
                        "toid": toid,
                        "version_date": version_date,
                        "source_product": source_product,
                        "geometry_wkt": geom_wkt,
                        "longitude": longitude,
                        "latitude": latitude,
                        "street_name": street_metadata.get("street_name"),
                        "locality": street_metadata.get("locality"),
                        "region": street_metadata.get("region"),
                        "postcode_area": street_metadata.get("postcode_area")
                    }
                    
                    toids_with_metadata.append(combined_data)
                    
                except Exception as e:
                    error_msg = f"Failed to get metadata for TOID {toid}: {e}"
                    logger.warning(error_msg)
                    results["errors"].append(error_msg)
                    
                    # Still add TOID data without metadata
                    combined_data = {
                        "toid": toid,
                        "version_date": version_date,
                        "source_product": source_product,
                        "geometry_wkt": geom_wkt,
                        "longitude": longitude,
                        "latitude": latitude,
                        "street_name": None,
                        "locality": None,
                        "region": None,
                        "postcode_area": None
                    }
                    
                    toids_with_metadata.append(combined_data)
            
            logger.info(f"Processed {len(toids_with_metadata)} TOIDs, {results['toids_with_metadata']} with metadata")
            
            # Step 3: Save to database
            logger.info("Step 3: Saving data to database...")
            saved_count = self._save_toids_to_database(toids_with_metadata)
            results["toids_saved"] = saved_count
            results["success"] = True
            
            logger.info(f"Processing completed: {saved_count} TOIDs saved to database")
            
        except Exception as e:
            error_msg = f"Processing failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            results["success"] = False
        
        return results
    
    def _save_toids_to_database(self, toids_data: List[Dict[str, Any]]) -> int:
        """Save TOID data with metadata to database.
        
        Args:
            toids_data: List of combined TOID and metadata dictionaries
            
        Returns:
            Number of TOIDs saved
        """
        saved_count = 0
        
        # Create/update toid_metadata table
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                # Create toid_metadata table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS toid_metadata (
                        id SERIAL PRIMARY KEY,
                        toid VARCHAR(36) UNIQUE NOT NULL,
                        version_date DATE,
                        source_product VARCHAR(100),
                        geom GEOMETRY(POINT, 4326),
                        longitude DOUBLE PRECISION NOT NULL,
                        latitude DOUBLE PRECISION NOT NULL,
                        street_name TEXT,
                        locality TEXT,
                        region TEXT,
                        postcode_area VARCHAR(10),
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create index on toid for fast lookups
                cursor.execute("""
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_indexes 
                            WHERE tablename = 'toid_metadata' AND indexname = 'idx_toid_metadata_toid'
                        ) THEN
                            CREATE INDEX idx_toid_metadata_toid ON toid_metadata(toid);
                        END IF;
                    END $$;
                """)
                
                # Create spatial index on geometry
                cursor.execute("""
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_indexes 
                            WHERE tablename = 'toid_metadata' AND indexname = 'idx_toid_metadata_geom'
                        ) THEN
                            CREATE INDEX idx_toid_metadata_geom ON toid_metadata USING GIST(geom);
                        END IF;
                    END $$;
                """)
                
        except Exception as e:
            logger.error(f"Failed to set up toid_metadata table: {e}")
            return saved_count
        
        # Save individual TOIDs
        for toid_data in toids_data:
            try:
                with self.db_service.transaction() as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                        INSERT INTO toid_metadata (
                            toid, version_date, source_product, geom, longitude, latitude,
                            street_name, locality, region, postcode_area
                        )
                        VALUES (%s, %s, %s, ST_MakePoint(%s, %s), %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (toid) DO UPDATE SET
                            version_date = EXCLUDED.version_date,
                            source_product = EXCLUDED.source_product,
                            geom = EXCLUDED.geom,
                            longitude = EXCLUDED.longitude,
                            latitude = EXCLUDED.latitude,
                            street_name = EXCLUDED.street_name,
                            locality = EXCLUDED.locality,
                            region = EXCLUDED.region,
                            postcode_area = EXCLUDED.postcode_area,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        toid_data["toid"],
                        toid_data["version_date"],
                        toid_data["source_product"],
                        toid_data["longitude"],
                        toid_data["latitude"],
                        toid_data["longitude"],
                        toid_data["latitude"],
                        toid_data["street_name"],
                        toid_data["locality"],
                        toid_data["region"],
                        toid_data["postcode_area"]
                    ))
                    
                    if cursor.rowcount > 0:
                        saved_count += 1
                        
            except Exception as e:
                logger.warning(f"Failed to save TOID {toid_data['toid']}: {e}")
                continue
        
        return saved_count
    
    def get_toid_metadata(self, toid: str) -> Optional[Dict[str, Any]]:
        """Get TOID metadata from database.
        
        Args:
            toid: TOID identifier
            
        Returns:
            TOID metadata dictionary or None if not found
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        toid, version_date, source_product,
                        longitude, latitude,
                        street_name, locality, region, postcode_area,
                        created_at, updated_at
                    FROM toid_metadata 
                    WHERE toid = %s
                """, (toid,))
                
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            logger.error(f"Failed to get TOID metadata for {toid}: {e}")
            return None
    
    def search_toids_by_location(
        self, 
        bbox: List[float], 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Search for TOIDs within a bounding box.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            limit: Optional limit on results
            
        Returns:
            List of TOID metadata dictionaries
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                query = """
                    SELECT 
                        toid, version_date, source_product,
                        longitude, latitude,
                        street_name, locality, region, postcode_area,
                        created_at, updated_at
                    FROM toid_metadata 
                    WHERE ST_Within(
                        geom,
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                    )
                    ORDER BY created_at DESC
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor.execute(query, bbox)
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to search TOIDs by location: {e}")
            return []
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get information about the TOID metadata service."""
        return {
            "service_name": "TOIDMetadataService",
            "workflow": [
                "1. Features API: Fetch TOIDs and coordinates",
                "2. Names API: Get street/location metadata", 
                "3. Database: Save combined data"
            ],
            "clients": {
                "features_client": self.features_client.get_service_info(),
                "names_client": self.names_client.get_service_info(),
                "database_service": "DatabaseService"
            }
        }