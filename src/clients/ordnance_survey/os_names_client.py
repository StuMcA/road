
import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)


class OSNamesClient:
    """Client for accessing Ordnance Survey Names API nearest endpoint.
    
    Fetches street and location metadata using coordinate-based lookups
    with support for filtering by location types.
    """
    
    BASE_URL = "https://api.os.uk/search/names/v1/nearest"
    DEFAULT_TIMEOUT = 30
    DEFAULT_RETRY_ATTEMPTS = 3
    RATE_LIMIT_DELAY = 0.1  # Seconds between requests
    
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
            'Accept': 'application/json'
        })

    def get_nearest_street_info(
        self, 
        easting: float, 
        northing: float, 
        radius: int = 100
    ) -> Optional[Dict[str, Any]]:
        """Get nearest street/location information for given BNG coordinates.
        
        Args:
            easting: British National Grid easting coordinate
            northing: British National Grid northing coordinate
            radius: Search radius in meters (max 1000)
            
        Returns:
            Dictionary with location information or None if not found
        """
        return self._get_nearest_with_retry(easting, northing, radius)

    def get_street_metadata(
        self, 
        easting: float, 
        northing: float, 
        radius: int = 100
    ) -> Dict[str, Optional[str]]:
        """Extract street and postcode metadata from BNG coordinates using Names API.
        
        Makes two separate API calls:
        1. Road/street data (TYPE, NAME1, ID, REGION, COUNTY_UNITARY)
        2. Postcode data (full POSTCODE)
        
        Args:
            easting: British National Grid easting coordinate
            northing: British National Grid northing coordinate
            radius: Search radius in meters
            
        Returns:
            Dictionary with TYPE, NAME1, ID, POSTCODE, REGION, COUNTY_UNITARY
        """
        result = {
            "TYPE": None,
            "NAME1": None,
            "ID": None,
            "POSTCODE": None,
            "REGION": None,
            "COUNTY_UNITARY": None
        }
        
        try:
            # First call: Get road/street information
            street_data = self.get_nearest_street_info(easting, northing, radius)
            
            if street_data:
                # Check if data is wrapped in GAZETTEER_ENTRY
                if "GAZETTEER_ENTRY" in street_data:
                    entry = street_data["GAZETTEER_ENTRY"]
                else:
                    entry = street_data
                
                # Extract road/street fields
                result["TYPE"] = entry.get("TYPE")
                result["NAME1"] = entry.get("NAME1") 
                result["ID"] = entry.get("ID")
                result["REGION"] = entry.get("REGION")
                result["COUNTY_UNITARY"] = entry.get("COUNTY_UNITARY")
                
        except Exception as e:
            logger.warning(f"Failed to get street data for BNG ({easting}, {northing}): {e}")

        try:
            # Second call: Get postcode information
            postcode_data = self.get_nearest_postcode_info(easting, northing, radius)
            
            if postcode_data:
                # Check if data is wrapped in GAZETTEER_ENTRY
                if "GAZETTEER_ENTRY" in postcode_data:
                    entry = postcode_data["GAZETTEER_ENTRY"]
                else:
                    entry = postcode_data
                
                # Extract postcode (NAME1 field contains the full postcode for postcode entries)
                result["POSTCODE"] = entry.get("NAME1")
                
        except Exception as e:
            logger.warning(f"Failed to get postcode data for BNG ({easting}, {northing}): {e}")
        
        # Map to expected field names
        return {
            "street_name": result.get("NAME1"),
            "local_authority": result.get("COUNTY_UNITARY"), 
            "region": result.get("REGION"),
            "postcode": result.get("POSTCODE")
        }


    def _get_nearest_with_retry(
        self, 
        easting: float, 
        northing: float, 
        radius: int
    ) -> Optional[Dict[str, Any]]:
        """Get nearest location with retry logic."""
        for attempt in range(self.retry_attempts):
            try:
                return self._get_nearest(easting, northing, radius)
            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    logger.debug(f"No locations found near BNG ({easting}, {northing})")
                    return None
                if attempt == self.retry_attempts - 1:
                    raise
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}")
                time.sleep(2 ** attempt)
            except requests.RequestException as e:
                if attempt == self.retry_attempts - 1:
                    raise
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}")
                time.sleep(2 ** attempt)
        
        return None

    def _get_nearest(
        self, 
        easting: float, 
        northing: float, 
        radius: int
    ) -> Optional[Dict[str, Any]]:
        """Get nearest location using Names API with BNG coordinates."""
        
        params = {
            "point": f"{easting:.2f},{northing:.2f}",  # BNG coordinates to 2 decimal places
            "radius": min(radius, 1000),  # Max 1000m radius
            "format": "JSON",
            "key": self.api_key
        }
        
        logger.debug(f"Searching for locations near BNG ({easting:.2f}, {northing:.2f}) within {radius}m")
        
        # Use space-separated multiple LOCAL_TYPE values for road types only
        params["fq"] = "LOCAL_TYPE:Named_Road LOCAL_TYPE:Numbered_Road LOCAL_TYPE:Section_Of_Named_Road LOCAL_TYPE:Section_Of_Numbered_Road"
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)
            
            logger.debug(f"Response status: {response.status_code}")
            
            if response.status_code == 404:
                return None
                
            if not response.ok:
                logger.error(f"API error: {response.text[:1000]}")
                response.raise_for_status()
            
            content_type = response.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                logger.error(f"Unexpected content type: {content_type}")
                raise ValueError(f"Expected JSON but got {content_type}")
                
            data = response.json()
            
            # Check if there's a single GAZETTEER_ENTRY (direct result)
            gazetteer_entry = data.get("GAZETTEER_ENTRY")
            if gazetteer_entry:
                return gazetteer_entry
            
            # Extract results (should be at most one result)
            results = data.get("results", [])
            if results and len(results) > 0:
                return results[0]
            
            return None
            
        except requests.Timeout:
            logger.error(f"Request timeout after {self.timeout} seconds")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        finally:
            # Rate limiting
            time.sleep(self.RATE_LIMIT_DELAY)


    def get_nearest_postcode_info(
        self, 
        easting: float, 
        northing: float, 
        radius: int = 100
    ) -> Optional[Dict[str, Any]]:
        """Get nearest postcode information for given BNG coordinates.
        
        Args:
            easting: British National Grid easting coordinate
            northing: British National Grid northing coordinate
            radius: Search radius in meters (max 1000)
            
        Returns:
            Dictionary with postcode information or None if not found
        """
        params = {
            "point": f"{easting:.2f},{northing:.2f}",  # BNG coordinates to 2 decimal places
            "radius": min(radius, 1000),  # Max 1000m radius
            "format": "JSON",
            "key": self.api_key
        }
        
        logger.debug(f"Searching for postcodes near BNG ({easting:.2f}, {northing:.2f}) within {radius}m")
        
        # Use LOCAL_TYPE for postcodes
        params["fq"] = "LOCAL_TYPE:Postcode"
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)
            
            logger.debug(f"Postcode response status: {response.status_code}")
            
            if response.status_code == 404:
                return None
                
            if not response.ok:
                logger.error(f"Postcode API error: {response.text[:1000]}")
                response.raise_for_status()
            
            content_type = response.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                logger.error(f"Unexpected content type: {content_type}")
                raise ValueError(f"Expected JSON but got {content_type}")
                
            data = response.json()
            
            # Check if there's a single GAZETTEER_ENTRY (direct result)
            gazetteer_entry = data.get("GAZETTEER_ENTRY")
            if gazetteer_entry:
                return gazetteer_entry
            
            # Extract results (should be at most one result)
            results = data.get("results", [])
            if results and len(results) > 0:
                return results[0]
            
            return None
            
        except requests.Timeout:
            logger.error(f"Postcode request timeout after {self.timeout} seconds")
            raise
        except requests.RequestException as e:
            logger.error(f"Postcode request failed: {e}")
            raise
        finally:
            # Rate limiting
            time.sleep(self.RATE_LIMIT_DELAY)

    def get_service_info(self) -> Dict[str, Any]:
        """Get information about the OS Names service."""
        return {
            "service_name": "OSNamesClient",
            "base_url": self.BASE_URL,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "rate_limit_delay": self.RATE_LIMIT_DELAY,
            "api_key_configured": bool(self.api_key)
        }
    