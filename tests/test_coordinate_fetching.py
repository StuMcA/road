"""
Test script for coordinate-based image fetching functionality.

Tests the integration of MapillaryClient, ImageFetcherService, and
RoadAnalysisPipeline for fetching and analyzing images from coordinates.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


sys.path.append(str(Path(__file__).parent.parent))

from src.services.image.fetcher_service import ImageFetcherService
from src.clients.mapillary.mapillary_client import MapillaryClient
from src.services.pipeline.road_analysis_pipeline import RoadAnalysisPipeline
from src.utils.coord_utils import bbox_from_point


class TestCoordinateFetching(unittest.TestCase):
    """Test coordinate-based image fetching functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_lat = 51.5007  # London
        self.test_lon = -0.1246
        self.test_radius = 100.0

        # Create temporary directory for test downloads
        self.temp_dir = tempfile.mkdtemp(prefix="test_mapillary_")

    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temporary directory
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_bbox_from_point(self):
        """Test bounding box calculation from coordinates."""
        bbox = bbox_from_point(self.test_lat, self.test_lon, self.test_radius)

        self.assertEqual(len(bbox), 4)
        min_lat, min_lon, max_lat, max_lon = bbox

        # Check that we get a valid bounding box
        self.assertLess(min_lat, self.test_lat)
        self.assertLess(max_lat, self.test_lat + 0.01)  # Reasonable upper bound
        self.assertLess(min_lon, self.test_lon)
        self.assertLess(max_lon, self.test_lon + 0.01)

        # Check bbox is centered around the point
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        self.assertAlmostEqual(center_lat, self.test_lat, places=5)
        self.assertAlmostEqual(center_lon, self.test_lon, places=5)


class TestMapillaryClientMocked(unittest.TestCase):
    """Test MapillaryClient with mocked API responses."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_mapillary_")

        # Mock API response data
        self.mock_image_data = [
            {
                "id": "test_image_1",
                "thumb_original_url": "https://example.com/image1.jpg",
                "geometry": {"coordinates": [-0.1246, 51.5007]},
                "captured_at": "2023-01-01T12:00:00Z",
                "compass_angle": 90.0,
            },
            {
                "id": "test_image_2",
                "thumb_original_url": "https://example.com/image2.jpg",
                "geometry": {"coordinates": [-0.1245, 51.5008]},
                "captured_at": "2023-01-01T12:05:00Z",
                "compass_angle": 180.0,
            },
        ]

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    @patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"})
    @patch("src.services.mapillary_client.requests.get")
    def test_fetch_images_success(self, mock_get):
        """Test successful image metadata fetching."""
        # Mock API response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": self.mock_image_data}
        mock_get.return_value = mock_response

        client = MapillaryClient()
        bbox = bbox_from_point(51.5007, -0.1246, 100)
        images = client.fetch_images(bbox, limit=2)

        self.assertEqual(len(images), 2)
        self.assertEqual(images[0]["id"], "test_image_1")
        self.assertEqual(images[1]["id"], "test_image_2")

        # Verify API was called with correct parameters
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        self.assertIn("access_token", call_args[1]["params"])
        self.assertEqual(call_args[1]["params"]["limit"], 2)

    @patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"})
    @patch("src.services.mapillary_client.requests.get")
    def test_download_image_success(self, mock_get):
        """Test successful single image download."""
        # Mock image download response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [b"fake_image_data"]
        mock_get.return_value = mock_response

        client = MapillaryClient()
        image_metadata = self.mock_image_data[0]

        file_path = client.download_image(image_metadata, self.temp_dir)

        # Check file was created
        self.assertTrue(Path(file_path).exists())
        self.assertEqual(Path(file_path).name, "test_image_1.jpg")

        # Check file contains expected data
        with open(file_path, "rb") as f:
            content = f.read()
        self.assertEqual(content, b"fake_image_data")


class TestImageFetcherService(unittest.TestCase):
    """Test ImageFetcherService functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_fetcher_")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    @patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"})
    @patch("src.services.image_fetcher.MapillaryClient")
    def test_fetch_images_at_point_success(self, mock_client_class):
        """Test successful image fetching at coordinates."""
        # Mock MapillaryClient
        mock_client = Mock()
        mock_client.fetch_images.return_value = [
            {"id": "img1", "thumb_original_url": "https://example.com/1.jpg"},
            {"id": "img2", "thumb_original_url": "https://example.com/2.jpg"},
        ]
        mock_client.download_images.return_value = [
            f"{self.temp_dir}/img1.jpg",
            f"{self.temp_dir}/img2.jpg",
        ]
        mock_client_class.return_value = mock_client

        # Create fake downloaded files
        for img_id in ["img1", "img2"]:
            Path(f"{self.temp_dir}/{img_id}.jpg").touch()

        fetcher = ImageFetcherService()
        result = fetcher.fetch_images_at_point(
            51.5007, -0.1246, radius_m=100, limit=2, output_dir=self.temp_dir
        )

        # Verify result structure
        self.assertTrue(result["success"])
        self.assertEqual(result["coordinates"]["lat"], 51.5007)
        self.assertEqual(result["coordinates"]["lon"], -0.1246)
        self.assertEqual(result["radius_m"], 100)
        self.assertEqual(result["images_found"], 2)
        self.assertEqual(result["images_downloaded"], 2)
        self.assertEqual(len(result["image_paths"]), 2)
        self.assertEqual(result["failed_downloads"], 0)

        # Verify bbox was calculated
        self.assertEqual(len(result["bbox"]), 4)

        # Verify timing and metadata
        self.assertGreater(result["processing_time_ms"], 0)
        self.assertIn("timestamp", result)
        self.assertIn("service_version", result)

    @patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"})
    @patch("src.services.image_fetcher.MapillaryClient")
    def test_fetch_images_at_point_no_images(self, mock_client_class):
        """Test fetching when no images are found."""
        # Mock MapillaryClient returning no images
        mock_client = Mock()
        mock_client.fetch_images.return_value = []
        mock_client_class.return_value = mock_client

        fetcher = ImageFetcherService()
        result = fetcher.fetch_images_at_point(51.5007, -0.1246, output_dir=self.temp_dir)

        # Verify result for no images case
        self.assertTrue(result["success"])
        self.assertEqual(result["images_found"], 0)
        self.assertEqual(result["images_downloaded"], 0)
        self.assertEqual(len(result["image_paths"]), 0)

    def test_service_info(self):
        """Test service info retrieval."""
        with patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"}):
            fetcher = ImageFetcherService(default_radius_m=200.0)
            info = fetcher.get_service_info()

            self.assertEqual(info["service_name"], "ImageFetcherService")
            self.assertEqual(info["default_radius_m"], 200.0)
            self.assertIn("version", info)
            self.assertIn("mapillary_api_available", info)


class TestPipelineIntegration(unittest.TestCase):
    """Test integration with RoadAnalysisPipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix="test_pipeline_")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_pipeline_with_fetcher_enabled(self):
        """Test pipeline initialization with fetcher enabled."""
        with patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"}):
            pipeline = RoadAnalysisPipeline(enable_fetcher=True)

            self.assertIsNotNone(pipeline.fetcher_service)

            # Test service info includes fetcher
            info = pipeline.get_service_info()
            self.assertIsNotNone(info["fetcher_service"])

    def test_pipeline_with_fetcher_disabled(self):
        """Test pipeline initialization with fetcher disabled."""
        pipeline = RoadAnalysisPipeline(enable_fetcher=False)

        self.assertIsNone(pipeline.fetcher_service)

        # Test service info shows no fetcher
        info = pipeline.get_service_info()
        self.assertIsNone(info["fetcher_service"])

        # Test process_coordinate raises error
        with self.assertRaises(ValueError) as context:
            pipeline.process_coordinate(51.5007, -0.1246)

        self.assertIn("Image fetcher service not enabled", str(context.exception))

    @patch.dict(os.environ, {"MAPILLARY_ACCESS_TOKEN": "test_token"})
    @patch("src.services.image_fetcher.ImageFetcherService.fetch_and_process_images")
    def test_process_coordinate_delegation(self, mock_fetch_and_process):
        """Test that process_coordinate properly delegates to fetcher service."""
        # Mock the fetch_and_process_images method
        expected_result = {
            "fetch_result": {"success": True, "images_downloaded": 2},
            "pipeline_results": {},
            "analysis_summary": {"total_images": 2},
        }
        mock_fetch_and_process.return_value = expected_result

        pipeline = RoadAnalysisPipeline(enable_fetcher=True)
        result = pipeline.process_coordinate(
            51.5007, -0.1246, radius_m=150, limit=5, output_dir=self.temp_dir
        )

        # Verify the method was called with correct parameters
        mock_fetch_and_process.assert_called_once_with(
            51.5007, -0.1246, pipeline, 150, 5, self.temp_dir
        )

        # Verify result is passed through
        self.assertEqual(result, expected_result)


def run_integration_test():
    """
    Manual integration test that requires actual Mapillary API access.
    Only run this if you have a valid MAPILLARY_ACCESS_TOKEN set.
    """
    if not os.getenv("MAPILLARY_ACCESS_TOKEN"):
        print("❌ MAPILLARY_ACCESS_TOKEN not set. Skipping integration test.")
        return

    print("🚀 Running integration test with real Mapillary API...")

    try:
        # Test coordinate in London (known to have street imagery)
        pipeline = RoadAnalysisPipeline(enable_fetcher=True)

        print("📍 Testing coordinates: 51.5007, -0.1246 (London)")
        print(f"🔧 Pipeline info: {pipeline.get_service_info()}")

        # Test with small radius and limit for quick test
        result = pipeline.process_coordinate(
            lat=51.5007,
            lon=-0.1246,
            radius_m=50,  # Small radius
            limit=2,  # Just 2 images
        )

        print("✅ Integration test completed!")
        print(f"📊 Fetch result: Found {result['fetch_result']['images_found']} images")
        print(f"📊 Downloaded: {result['fetch_result']['images_downloaded']} images")
        print(f"📊 Pipeline processed: {len(result['pipeline_results'])} images")
        print(f"📊 Success rate: {result['analysis_summary']['success_rate']:.1f}%")

        # Clean up downloaded images
        output_dir = result["fetch_result"]["output_dir"]
        if Path(output_dir).exists():
            import shutil

            shutil.rmtree(output_dir)
            print(f"🧹 Cleaned up: {output_dir}")

    except Exception as e:
        print(f"❌ Integration test failed: {e}")


if __name__ == "__main__":
    print("🧪 Running coordinate fetching tests...")

    # Run unit tests
    unittest.main(argv=[""], exit=False, verbosity=2)

    print("\n" + "=" * 50)

    # Run integration test if API token is available
    run_integration_test()
