import logging
import os
import time
from datetime import datetime
from typing import Any, List, Optional, Tuple

import requests
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)

class OSFeaturesClient:
    """Client for accessing Ordnance Survey Features API.
    
    Provides access to OpenTOID_HighwaysNetwork features with pagination,
    error handling, and rate limiting.
    """
    
    BASE_URL = "https://api.os.uk/features/v1/wfs"
    DEFAULT_TIMEOUT = 30
    DEFAULT_RETRY_ATTEMPTS = 3
    RATE_LIMIT_DELAY = 0.1  # Seconds between requests
    MAX_COUNT_PER_REQUEST = 1000
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT, retry_attempts: int = DEFAULT_RETRY_ATTEMPTS):
        self.api_key = os.getenv("OS_API_KEY")
        if not self.api_key:
            raise ValueError("OS_API_KEY not set in environment")
        
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()
        
        # Set session defaults
        self.session.headers.update({
            'User-Agent': 'Road-Quality-Analysis/1.0', 
            'Accept': 'application/json',
            'key': self.api_key  # API key goes in header, not query params
        })

    def fetch_all_toid_features(
        self, 
        bbox: List[float], 
        count: int = 100,
        max_features: Optional[int] = None
    ) -> List[Tuple[str, Optional[datetime], str, str, float, float, float, float]]:
        """Fetch all TOID features within a bounding box with pagination.
        
        Args:
            bbox: Bounding box as [min_lon, min_lat, max_lon, max_lat]
            count: Number of features per request (max 1000)
            max_features: Optional limit on total features to fetch
            
        Returns:
            List of tuples: (toid, version_date, source_product, geom_wkt, longitude, latitude, easting, northing)
            
        Raises:
            ValueError: If bbox is invalid or API returns error
            requests.RequestException: If API request fails
        """
        self._validate_bbox(bbox)
        count = min(count, self.MAX_COUNT_PER_REQUEST)
        
        all_features = []
        offset = 0

        while True:
            logger.info(f"Fetching page with offset {offset}")

            response = self._fetch_toid_features_with_retry(bbox, count, offset)
            all_features.extend(response)

            logger.info(f"Received {len(response)} features")

            # Check stopping conditions
            if len(response) < count:
                logger.info(f"Last page reached. Total: {len(all_features)} features")
                break
                
            if max_features and len(all_features) >= max_features:
                logger.info(f"Max features limit reached: {len(all_features)}")
                all_features = all_features[:max_features]
                break

            offset += count
            logger.info(f"{len(all_features)} found so far")
            
            # Rate limiting
            time.sleep(self.RATE_LIMIT_DELAY)

        return all_features




    def _fetch_toid_features_with_retry(
        self, 
        bbox: List[float], 
        count: int, 
        start: int
    ) -> List[Tuple[str, Optional[datetime], str, str, float, float]]:
        """Fetch TOID features with retry logic."""
        for attempt in range(self.retry_attempts):
            try:
                return self._fetch_toid_features(bbox, count, start)
            except requests.RequestException as e:
                if attempt == self.retry_attempts - 1:
                    raise
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return []  # Should never reach here
    
    def _fetch_toid_features(
        self, 
        bbox: List[float], 
        count: int, 
        start: int
    ) -> List[Tuple[str, Optional[datetime], str, str, float, float]]:
        """Fetch a single page of TOID features."""
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "OpenTOID_HighwaysNetwork",
            "srsName": "EPSG:4326",
            "outputFormat": "GEOJSON",
            "bbox": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",  # Format: min_lon,min_lat,max_lon,max_lat
            "count": count,
            "startIndex": start,
        }
        logger.debug(f"Fetching {count} features from index {start}")
        
        try:
            response = self.session.get(
                self.BASE_URL, 
                params=params, 
                timeout=self.timeout
            )
            
            logger.debug(f"Response status: {response.status_code}")
            
            if not response.ok:
                logger.error(f"API error: {response.text[:1000]}")
                response.raise_for_status()
            
            content_type = response.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                logger.error(f"Unexpected content type: {content_type}")
                logger.error(f"Response: {response.text[:1000]}")
                raise ValueError(f"Expected JSON but got {content_type}")
                
            data = response.json()
            if "features" not in data:
                logger.error(f"No features in response: {data}")
                raise ValueError("API response missing 'features' field")
                
            return self._parse_toid_features(data["features"])
            
        except requests.Timeout:
            logger.error(f"Request timeout after {self.timeout} seconds")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def _parse_toid_features(
        self, 
        features: List[dict]
    ) -> List[Tuple[str, Optional[datetime], str, str, float, float, float, float]]:
        """Parse TOID features from GeoJSON response."""
        data = []
        
        for i, feature in enumerate(features):
            try:
                props = feature.get("properties", {})
                geometry = feature.get("geometry", {})
                
                # Extract required fields
                easting = props.get("Easting")
                northing = props.get("Northing")
                toid = props.get("TOID")
                version_date = props.get("VersionDate")
                source_product = props.get("SourceProduct", "unknown")
                
                # Extract coordinates
                coordinates = geometry.get("coordinates")
                if not coordinates or len(coordinates) < 2:
                    logger.warning(f"Feature {i}: Invalid coordinates")
                    continue
                    
                longitude = float(coordinates[0])
                latitude = float(coordinates[1])
                
                # Validate required fields
                if not all([easting, northing, toid]):
                    logger.warning(f"Feature {i}: Missing required fields (easting={easting}, northing={northing}, toid={toid})")
                    continue
                
                # Parse date
                parsed_date = self._parse_version_date(version_date)
                
                # Create WKT geometry
                geom_wkt = f"SRID=27700;POINT({easting} {northing})"
                
                data.append((
                    str(toid),
                    parsed_date,
                    str(source_product),
                    geom_wkt,
                    longitude,
                    latitude,
                    float(easting),
                    float(northing)
                ))
                
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error parsing feature {i}: {e}")
                continue
                
        logger.debug(f"Parsed {len(data)} valid features from {len(features)} total")
        return data

    
    def _parse_version_date(self, version_date: Any) -> Optional[datetime]:
        """Parse version date from various formats."""
        if not version_date:
            return None
            
        if isinstance(version_date, datetime):
            return version_date
            
        if isinstance(version_date, str):
            # Try common date formats
            for date_format in ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(version_date, date_format)
                except ValueError:
                    continue
                    
        logger.warning(f"Could not parse version date: {version_date}")
        return None
    
    def _validate_bbox(self, bbox: List[float]) -> None:
        """Validate bounding box format and values."""
        if not isinstance(bbox, list) or len(bbox) != 4:
            raise ValueError("bbox must be a list of 4 coordinates [min_lon, min_lat, max_lon, max_lat]")
            
        min_lon, min_lat, max_lon, max_lat = bbox
        
        if not all(isinstance(coord, (int, float)) for coord in bbox):
            raise ValueError("All bbox coordinates must be numeric")
            
        if min_lon >= max_lon:
            raise ValueError(f"min_lon ({min_lon}) must be less than max_lon ({max_lon})")
            
        if min_lat >= max_lat:
            raise ValueError(f"min_lat ({min_lat}) must be less than max_lat ({max_lat})")
            
        # Basic UK bounds check
        if not (-8 <= min_lon <= 2 and 49 <= min_lat <= 61):
            logger.warning(f"bbox appears to be outside UK bounds: {bbox}")
    
    def get_service_info(self) -> dict[str, Any]:
        """Get information about the OS Features service."""
        return {
            "service_name": "OSFeaturesClient",
            "base_url": self.BASE_URL,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "rate_limit_delay": self.RATE_LIMIT_DELAY,
            "max_count_per_request": self.MAX_COUNT_PER_REQUEST,
            "api_key_configured": bool(self.api_key)
        }
