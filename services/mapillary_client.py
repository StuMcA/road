import requests
import os
from typing import List, Dict
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class MapillaryClient:
    BASE_URL = "https://graph.mapillary.com/images"

    def __init__(self):
        self.access_token = os.getenv("MAPILLARY_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("MAPILLARY_CLIENT_TOKEN not set in environment")

    def fetch_images(
        self,
        bbox,
        limit: int = 5,
        fields: str = "id,thumb_original_url,geometry,captured_at,compass_angle"
    ) -> List[Dict]:
        """Fetch nearby street-level images from Mapillary."""
        params = {
            "access_token": self.access_token,
            "fields": fields,
            "limit": limit
        }
        response = requests.get(f"{self.BASE_URL}?bbox={",".join([str(i) for i in bbox])}", params=params)
        response.raise_for_status()
        return response.json().get("data", [])

    def print_images(self, images: List[Dict]) -> None:
        """Pretty-print image metadata."""
        for img in images:
            print(f"ID: {img['id']}")
            print(f"Captured at: {img['captured_at']}")
            print(f"Location: {img['geometry']['coordinates']}")
            print(f"Compass angle: {img['compass_angle']}")
            print(f"Thumbnail URL: {img['thumb_original_url']}")
            print("-" * 40)


    def download_images(self, images: List[Dict], output_dir: str = "mapillary_images") -> None:
        """Download thumbnail images to a local directory."""
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        for img in images:
            img_id = img["id"]
            url = img["thumb_original_url"]
            file_path = Path(output_dir) / f"{img_id}.jpg"

            print(f"Downloading {img_id}...")
            try:
                r = requests.get(url, stream=True)
                r.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Saved to {file_path}")
            except requests.RequestException as e:
                print(f"Failed to download {img_id}: {e}")
