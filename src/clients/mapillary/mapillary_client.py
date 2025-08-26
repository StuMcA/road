import os
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()


class MapillaryClient:
    BASE_URL = "https://graph.mapillary.com/images"

    def __init__(self):
        self.access_token = os.getenv("MAPILLARY_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("MAPILLARY_ACCESS_TOKEN not set in environment")

    def fetch_images(
        self,
        bbox: tuple,
        limit: int = 5,
        fields: str = "id,thumb_original_url,geometry,captured_at,compass_angle",
    ) -> list[dict]:
        """Fetch nearby street-level images from Mapillary.

        Args:
            bbox: Bounding box as (min_lat, min_lon, max_lat, max_lon)
            limit: Maximum number of images to fetch
            fields: Comma-separated fields to include in response

        Returns:
            List of image metadata dictionaries
        """
        # Convert bbox from (min_lat, min_lon, max_lat, max_lon)
        # to Mapillary format: left,bottom,right,top (minLon,minLat,maxLon,maxLat)
        min_lat, min_lon, max_lat, max_lon = bbox
        mapillary_bbox = f"{min_lon},{min_lat},{max_lon},{max_lat}"

        params = {
            "access_token": self.access_token, 
            "fields": fields, 
            "limit": limit,
            "bbox": mapillary_bbox
        }

        response = requests.get(self.BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get("data", [])

    def download_image(self, image_metadata: dict, output_dir: str = "mapillary_images") -> str:
        """Download a single image and return the local file path.

        Args:
            image_metadata: Image metadata dict with id and thumb_original_url
            output_dir: Directory to save downloaded images

        Returns:
            Path to downloaded image file

        Raises:
            requests.RequestException: If download fails
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        img_id = image_metadata["id"]
        url = image_metadata["thumb_original_url"]
        file_path = Path(output_dir) / f"{img_id}.jpg"

        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return str(file_path)

    def download_images(
        self, images: list[dict], output_dir: str = "mapillary_images"
    ) -> list[str]:
        """Download multiple images and return list of local file paths.

        Args:
            images: List of image metadata dictionaries
            output_dir: Directory to save downloaded images

        Returns:
            List of paths to successfully downloaded images
        """
        downloaded_paths = []

        for img in images:
            try:
                file_path = self.download_image(img, output_dir)
                downloaded_paths.append(file_path)
            except requests.RequestException:
                # Skip failed downloads, continue with others
                continue

        return downloaded_paths
