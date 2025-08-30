import tempfile
import time
from pathlib import Path
from typing import Optional

from .mapillary_client import MapillaryClient
from src.utils.coord_utils import bbox_from_point


class ImageFetcherService:
    """
    Service for fetching street-level images from coordinate points.

    Uses Mapillary API to fetch images within a bounding box centered
    on the provided coordinates.
    """

    def __init__(self, default_radius_m: float = 100.0):
        """
        Initialize the image fetcher service.

        Args:
            default_radius_m: Default radius in meters for bounding box creation
        """
        self.mapillary_client = MapillaryClient()
        self.default_radius_m = default_radius_m
        self.version = "1.0.0"

    def fetch_images_at_point(
        self,
        lat: float,
        lon: float,
        radius_m: Optional[float] = None,
        limit: int = 10,
        output_dir: Optional[str] = None,
    ) -> dict:
        """
        Fetch images at a coordinate point within specified radius.

        Args:
            lat: Latitude in degrees
            lon: Longitude in degrees
            radius_m: Radius in meters for image search (uses default if None)
            limit: Maximum number of images to fetch
            output_dir: Directory to download images (uses temp dir if None)

        Returns:
            Dictionary with fetch results including image paths and metadata
        """
        start_time = time.time()

        # Use default radius if not specified
        if radius_m is None:
            radius_m = self.default_radius_m

        # Create output directory
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="mapillary_images_")
        else:
            Path(output_dir).mkdir(parents=True, exist_ok=True)

        try:
            # Create bounding box from point and radius
            bbox = bbox_from_point(lat, lon, radius_m)

            # Fetch image metadata from Mapillary
            image_metadata = self.mapillary_client.fetch_images(bbox, limit=limit)

            if not image_metadata:
                processing_time = (time.time() - start_time) * 1000
                return {
                    "success": True,
                    "coordinates": {"lat": lat, "lon": lon},
                    "radius_m": radius_m,
                    "bbox": bbox,
                    "images_found": 0,
                    "images_downloaded": 0,
                    "image_paths": [],
                    "failed_downloads": 0,
                    "output_dir": output_dir,
                    "processing_time_ms": processing_time,
                    "timestamp": time.time(),
                    "service_version": self.version,
                }

            # Download images
            downloaded_paths = self.mapillary_client.download_images(image_metadata, output_dir)

            processing_time = (time.time() - start_time) * 1000

            return {
                "success": True,
                "coordinates": {"lat": lat, "lon": lon},
                "radius_m": radius_m,
                "bbox": bbox,
                "images_found": len(image_metadata),
                "images_downloaded": len(downloaded_paths),
                "image_paths": downloaded_paths,
                "failed_downloads": len(image_metadata) - len(downloaded_paths),
                "output_dir": output_dir,
                "processing_time_ms": processing_time,
                "timestamp": time.time(),
                "service_version": self.version,
                "image_metadata": image_metadata,
            }

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000
            return {
                "success": False,
                "coordinates": {"lat": lat, "lon": lon},
                "radius_m": radius_m,
                "error": str(e),
                "images_found": 0,
                "images_downloaded": 0,
                "image_paths": [],
                "failed_downloads": 0,
                "output_dir": output_dir,
                "processing_time_ms": processing_time,
                "timestamp": time.time(),
                "service_version": self.version,
            }

    def fetch_and_process_images(
        self,
        lat: float,
        lon: float,
        pipeline,
        radius_m: Optional[float] = None,
        limit: int = 10,
        output_dir: Optional[str] = None,
    ) -> dict:
        """
        Fetch images at coordinates and process through analysis pipeline.

        Args:
            lat: Latitude in degrees
            lon: Longitude in degrees
            pipeline: RoadAnalysisPipeline instance for processing
            radius_m: Radius in meters for image search
            limit: Maximum number of images to fetch
            output_dir: Directory to download images

        Returns:
            Dictionary with fetch results and pipeline analysis results
        """
        # First fetch the images
        fetch_result = self.fetch_images_at_point(lat, lon, radius_m, limit, output_dir)

        if not fetch_result["success"] or not fetch_result["image_paths"]:
            return {
                "fetch_result": fetch_result,
                "pipeline_results": {},
                "analysis_summary": {
                    "total_images": 0,
                    "processed_successfully": 0,
                    "failed_quality_check": 0,
                    "success_rate": 0,
                },
            }

        # Process images through pipeline
        pipeline_results = pipeline.process_batch(fetch_result["image_paths"])
        analysis_summary = pipeline.get_pipeline_stats(pipeline_results)

        return {
            "fetch_result": fetch_result,
            "pipeline_results": pipeline_results,
            "analysis_summary": analysis_summary,
        }

    def get_service_info(self) -> dict:
        """Get information about the image fetcher service."""
        return {
            "service_name": "ImageFetcherService",
            "version": self.version,
            "default_radius_m": self.default_radius_m,
            "mapillary_api_available": hasattr(self.mapillary_client, "access_token")
            and self.mapillary_client.access_token is not None,
        }
