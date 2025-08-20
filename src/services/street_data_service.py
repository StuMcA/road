"""
Street Data Service

Consolidated service that utilizes both OS clients to construct a comprehensive
dataset of street sections with coordinates and metadata for database storage.

This service replaces:
- toid_metadata_service
- street_pipeline_service  
- road_sections_service
- Coordinate fetching aspects of toid_analysis_pipeline
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..clients.ordnance_survey.os_features_client import OSFeaturesClient
from ..clients.ordnance_survey.os_names_client import OSNamesClient
from ..database.services.database_service import DatabaseService


logger = logging.getLogger(__name__)


class StreetDataService:
    """
    Unified service for comprehensive street data collection and storage.
    
    Workflow:
    1. Fetch TOIDs and coordinates from OS Features API
    2. Enrich with street metadata from OS Names API
    3. Save comprehensive dataset to database
    """
    
    def __init__(self):
        """Initialize the street data service with both OS clients and database."""
        self.features_client = OSFeaturesClient()
        self.names_client = OSNamesClient()
        self.db_service = DatabaseService()
        
        self.version = "1.0.0"
        
        # Ensure database schema is set up on initialization
        try:
            self._ensure_street_data_table()
        except Exception as e:
            logger.warning(f"Could not ensure street_data table setup on init: {e}")
            
        logger.info("StreetDataService initialized")
    
    def collect_street_data(
        self, 
        bbox: List[float], 
        max_features: Optional[int] = None,
        metadata_radius_m: int = 100
    ) -> Dict[str, Any]:
        """
        Collect comprehensive street data for an area.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            max_features: Optional limit on number of TOIDs to process
            metadata_radius_m: Search radius in meters for street metadata
            
        Returns:
            Collection results with statistics and data
        """
        start_time = datetime.now()
        logger.info(f"Starting street data collection for bbox: {bbox}")
        
        results = {
            "success": False,
            "bbox": bbox,
            "start_time": start_time.isoformat(),
            "features_fetched": 0,
            "features_with_metadata": 0,
            "features_saved": 0,
            "processing_errors": [],
            "metadata": {
                "service_version": self.version,
                "metadata_radius_m": metadata_radius_m
            }
        }
        
        try:
            # Step 1: Fetch TOID features from OS Features API
            logger.info("Fetching TOID features from OS Features API...")
            toid_features = self.features_client.fetch_all_toid_features(
                bbox=bbox,
                max_features=max_features
            )
            
            results["features_fetched"] = len(toid_features)
            logger.info(f"Fetched {len(toid_features)} TOID features")
            
            if not toid_features:
                logger.warning("No TOID features found in specified area")
                results["success"] = True
                results["end_time"] = datetime.now().isoformat()
                return results
            
            # Step 2: Enrich with street metadata from OS Names API
            logger.info("Enriching features with street metadata...")
            enriched_features = self._enrich_with_metadata(
                toid_features, metadata_radius_m, results
            )
            
            results["features_with_metadata"] = sum(
                1 for feature in enriched_features 
                if any([
                    feature.get("street_name"),
                    feature.get("locality"),
                    feature.get("region")
                ])
            )
            
            # Step 3: Save to database
            logger.info("Saving enriched features to database...")
            saved_count = self._save_street_data(enriched_features, results)
            results["features_saved"] = saved_count
            
            # Finalize results
            results["success"] = True
            results["end_time"] = datetime.now().isoformat()
            
            processing_time = (datetime.now() - start_time).total_seconds()
            results["processing_time_seconds"] = processing_time
            
            logger.info(
                f"Street data collection completed: {saved_count}/{len(toid_features)} "
                f"features saved with {results['features_with_metadata']} having metadata"
            )
            
        except Exception as e:
            error_msg = f"Street data collection failed: {e}"
            logger.error(error_msg)
            results["processing_errors"].append({
                "error": error_msg,
                "timestamp": datetime.now().isoformat()
            })
            results["end_time"] = datetime.now().isoformat()
        
        return results
    
    def _enrich_with_metadata(
        self, 
        toid_features: List[Tuple], 
        radius_m: int,
        results: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Enrich TOID features with street metadata from OS Names API.
        
        Args:
            toid_features: List of TOID feature tuples from Features API
            radius_m: Search radius in meters for metadata
            results: Results dictionary to update with errors
            
        Returns:
            List of enriched feature dictionaries
        """
        enriched_features = []
        total_features = len(toid_features)
        
        for i, feature_tuple in enumerate(toid_features, 1):
            # Unpack feature data - handle both 6 and 8 element tuples
            if len(feature_tuple) == 8:
                toid, version_date, source_product, geom_wkt, longitude, latitude, easting, northing = feature_tuple
            else:
                # Fallback for 6-element tuples (missing easting/northing)
                toid, version_date, source_product, geom_wkt, longitude, latitude = feature_tuple
                easting, northing = None, None
            
            logger.debug(f"Processing feature {i}/{total_features}: {toid}")
            
            # Create base feature data
            feature_data = {
                "toid": toid,
                "version_date": version_date,
                "source_product": source_product,
                "geometry_wkt": geom_wkt,
                "longitude": longitude,
                "latitude": latitude,
                "easting": easting,
                "northing": northing,
                "street_name": None,
                "locality": None,
                "region": None,
                "postcode_area": None
            }
            
            # Try to get street metadata if we have BNG coordinates
            if easting and northing:
                try:
                    street_metadata = self.names_client.get_street_metadata(
                        easting=easting,
                        northing=northing,
                        radius=radius_m
                    )
                    
                    # Update feature with metadata
                    feature_data.update({
                        "street_name": street_metadata.get("street_name"),
                        "locality": street_metadata.get("locality"),
                        "region": street_metadata.get("region"),
                        "postcode_area": street_metadata.get("postcode_area")
                    })
                    
                    if any(street_metadata.values()):
                        logger.debug(f"Found metadata for TOID {toid}")
                    
                except Exception as e:
                    error_msg = f"Failed to get metadata for TOID {toid}: {e}"
                    logger.warning(error_msg)
                    results["processing_errors"].append({
                        "toid": toid,
                        "error": error_msg,
                        "timestamp": datetime.now().isoformat()
                    })
            else:
                logger.debug(f"No BNG coordinates available for TOID {toid}, skipping metadata lookup")
            
            enriched_features.append(feature_data)
            
            # Progress logging
            if i % 50 == 0 or i == total_features:
                logger.info(f"Processed {i}/{total_features} features for metadata")
        
        return enriched_features
    
    def _save_street_data(
        self, 
        features: List[Dict[str, Any]], 
        results: Dict[str, Any]
    ) -> int:
        """
        Save enriched street data to database.
        
        Args:
            features: List of enriched feature dictionaries
            results: Results dictionary to update with errors
            
        Returns:
            Number of features successfully saved
        """
        saved_count = 0
        
        # Ensure street_data table exists
        try:
            self._ensure_street_data_table()
        except Exception as e:
            error_msg = f"Failed to ensure street_data table: {e}"
            logger.error(error_msg)
            results["processing_errors"].append({
                "error": error_msg,
                "timestamp": datetime.now().isoformat()
            })
            return saved_count
        
        # Save individual features
        for feature in features:
            try:
                with self.db_service.transaction() as conn:
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                        INSERT INTO street_data (
                            toid, version_date, source_product, 
                            geom, longitude, latitude, easting, northing,
                            street_name, locality, region, postcode_area
                        )
                        VALUES (
                            %s, %s, %s, 
                            ST_MakePoint(%s, %s), %s, %s, %s, %s,
                            %s, %s, %s, %s
                        )
                        ON CONFLICT (toid) DO UPDATE SET
                            version_date = EXCLUDED.version_date,
                            source_product = EXCLUDED.source_product,
                            geom = EXCLUDED.geom,
                            longitude = EXCLUDED.longitude,
                            latitude = EXCLUDED.latitude,
                            easting = EXCLUDED.easting,
                            northing = EXCLUDED.northing,
                            street_name = EXCLUDED.street_name,
                            locality = EXCLUDED.locality,
                            region = EXCLUDED.region,
                            postcode_area = EXCLUDED.postcode_area,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        feature["toid"],
                        feature["version_date"],
                        feature["source_product"],
                        feature["longitude"],
                        feature["latitude"],
                        feature["longitude"],
                        feature["latitude"],
                        feature["easting"],
                        feature["northing"],
                        feature["street_name"],
                        feature["locality"],
                        feature["region"],
                        feature["postcode_area"]
                    ))
                    
                    if cursor.rowcount > 0:
                        saved_count += 1
                        
            except Exception as e:
                error_msg = f"Failed to save feature {feature['toid']}: {e}"
                logger.warning(error_msg)
                results["processing_errors"].append({
                    "toid": feature["toid"],
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat()
                })
                continue
        
        logger.info(f"Saved {saved_count}/{len(features)} features to database")
        return saved_count
    
    def _ensure_street_data_table(self):
        """Ensure the street_data table exists with proper schema and indexes."""
        with self.db_service.transaction() as conn:
            cursor = conn.cursor()
            
            # Create street_data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS street_data (
                    id SERIAL PRIMARY KEY,
                    toid VARCHAR(36) UNIQUE NOT NULL,
                    version_date DATE,
                    source_product VARCHAR(100),
                    geom GEOMETRY(POINT, 4326),
                    longitude DOUBLE PRECISION NOT NULL,
                    latitude DOUBLE PRECISION NOT NULL,
                    easting INTEGER,
                    northing INTEGER,
                    street_name TEXT,
                    locality TEXT,
                    region TEXT,
                    postcode_area VARCHAR(10),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add street_data_id foreign key to photos table if it doesn't exist
            cursor.execute("""
                DO $$ 
                BEGIN
                    -- Check if column exists
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'photos' AND column_name = 'street_data_id'
                    ) THEN
                        ALTER TABLE photos ADD COLUMN street_data_id INTEGER REFERENCES street_data(id) ON DELETE SET NULL;
                    END IF;
                END $$;
            """)
            
            # Create indexes for efficient querying
            cursor.execute("""
                DO $$ 
                BEGIN
                    -- Index on TOID for fast lookups
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'street_data' AND indexname = 'idx_street_data_toid'
                    ) THEN
                        CREATE INDEX idx_street_data_toid ON street_data(toid);
                    END IF;
                    
                    -- Spatial index on geometry
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'street_data' AND indexname = 'idx_street_data_geom'
                    ) THEN
                        CREATE INDEX idx_street_data_geom ON street_data USING GIST(geom);
                    END IF;
                    
                    -- Index on street_name for text searches
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'street_data' AND indexname = 'idx_street_data_street_name'
                    ) THEN
                        CREATE INDEX idx_street_data_street_name ON street_data(street_name);
                    END IF;
                    
                    -- Index on locality for location filtering
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'street_data' AND indexname = 'idx_street_data_locality'
                    ) THEN
                        CREATE INDEX idx_street_data_locality ON street_data(locality);
                    END IF;
                    
                    -- Index on photos.street_data_id for fast lookups
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'photos' AND indexname = 'idx_photos_street_data_id'
                    ) THEN
                        CREATE INDEX idx_photos_street_data_id ON photos(street_data_id);
                    END IF;
                END $$;
            """)
    
    def query_street_data(
        self, 
        bbox: Optional[List[float]] = None,
        street_name: Optional[str] = None,
        locality: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query street data from database with various filters.
        
        Args:
            bbox: Optional bounding box [min_lon, min_lat, max_lon, max_lat]
            street_name: Optional street name filter (partial match)
            locality: Optional locality filter (partial match)
            limit: Optional limit on results
            
        Returns:
            List of street data dictionaries
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                # Build dynamic query
                where_clauses = []
                params = []
                
                if bbox:
                    where_clauses.append("""
                        ST_Within(geom, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                    """)
                    params.extend(bbox)
                
                if street_name:
                    where_clauses.append("street_name ILIKE %s")
                    params.append(f"%{street_name}%")
                
                if locality:
                    where_clauses.append("locality ILIKE %s")
                    params.append(f"%{locality}%")
                
                query = """
                    SELECT 
                        toid, version_date, source_product,
                        longitude, latitude, easting, northing,
                        street_name, locality, region, postcode_area,
                        created_at, updated_at
                    FROM street_data
                """
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                
                query += " ORDER BY created_at DESC"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Failed to query street data: {e}")
            return []
    
    def find_nearest_street_data(
        self, 
        lat: float, 
        lon: float, 
        max_distance_m: float = 500.0
    ) -> Optional[Dict[str, Any]]:
        """
        Find the nearest street data entry to a given location.
        
        Args:
            lat: Latitude
            lon: Longitude
            max_distance_m: Maximum search distance in meters
            
        Returns:
            Nearest street data dictionary or None if none found within distance
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        id, toid, version_date, source_product,
                        longitude, latitude, easting, northing,
                        street_name, locality, region, postcode_area,
                        created_at, updated_at,
                        ST_Distance(geom::geography, ST_MakePoint(%s, %s)::geography) as distance_m
                    FROM street_data 
                    WHERE ST_DWithin(geom::geography, ST_MakePoint(%s, %s)::geography, %s)
                    ORDER BY distance_m ASC
                    LIMIT 1
                """, (lon, lat, lon, lat, max_distance_m))
                
                result = cursor.fetchone()
                return dict(result) if result else None
                
        except Exception as e:
            logger.error(f"Failed to find nearest street data for {lat}, {lon}: {e}")
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about stored street data.
        
        Returns:
            Statistics dictionary
        """
        try:
            with self.db_service.transaction() as conn:
                cursor = conn.cursor()
                
                # Get basic counts
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_features,
                        COUNT(street_name) as features_with_street_name,
                        COUNT(locality) as features_with_locality,
                        COUNT(region) as features_with_region,
                        COUNT(DISTINCT street_name) as unique_street_names,
                        COUNT(DISTINCT locality) as unique_localities,
                        COUNT(DISTINCT region) as unique_regions
                    FROM street_data
                """)
                
                stats = dict(cursor.fetchone())
                
                # Get data coverage percentages
                if stats["total_features"] > 0:
                    stats["metadata_coverage"] = {
                        "street_name_pct": (stats["features_with_street_name"] / stats["total_features"]) * 100,
                        "locality_pct": (stats["features_with_locality"] / stats["total_features"]) * 100,
                        "region_pct": (stats["features_with_region"] / stats["total_features"]) * 100
                    }
                else:
                    stats["metadata_coverage"] = {
                        "street_name_pct": 0,
                        "locality_pct": 0,
                        "region_pct": 0
                    }
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get street data statistics: {e}")
            return {"error": str(e)}
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get information about the street data service."""
        return {
            "service_name": "StreetDataService",
            "version": self.version,
            "description": "Unified service for comprehensive street data collection and storage",
            "workflow": [
                "1. Fetch TOIDs and coordinates from OS Features API",
                "2. Enrich with street metadata from OS Names API", 
                "3. Save comprehensive dataset to database"
            ],
            "replaces": [
                "toid_metadata_service",
                "street_pipeline_service",
                "road_sections_service",
                "Coordinate aspects of toid_analysis_pipeline"
            ],
            "clients": {
                "features_client": self.features_client.get_service_info(),
                "names_client": self.names_client.get_service_info(),
                "database_service": "DatabaseService"
            },
            "database_table": "street_data"
        }