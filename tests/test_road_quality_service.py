"""
Unit tests for the road quality service components.

This test suite covers all aspects of road quality assessment including:
- Service initialization and configuration
- Model loading and validation
- Image preprocessing
- Quality metrics computation
- Error handling and edge cases
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
from PIL import Image


sys.path.append(str(Path(__file__).parent.parent))

from src.services.road_quality import RoadQualityMetrics, RoadQualityService
from src.services.road_quality.model_factory import ModelFactory
from src.services.road_quality.preprocessor import ImagePreprocessor
from src.services.road_quality.yolo_model import YOLOv8RoadModel


class TestRoadQualityMetrics(unittest.TestCase):
    """Test RoadQualityMetrics data class and its methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_metrics = RoadQualityMetrics(
            overall_quality_score=75.5,
            crack_confidence=0.8,
            crack_severity="moderate",
            pothole_confidence=0.2,
            pothole_count=1,
            surface_roughness=0.3,
            lane_marking_visibility=0.9,
            debris_score=0.1,
            weather_condition="dry",
            assessment_confidence=0.85,
            timestamp="2024-01-01T12:00:00Z",
            model_name="YOLOv8RoadQuality",
            model_version="1.0.0",
        )

    def test_metrics_initialization(self):
        """Test proper initialization of RoadQualityMetrics."""
        metrics = self.sample_metrics

        self.assertEqual(metrics.overall_quality_score, 75.5)
        self.assertEqual(metrics.crack_severity, "moderate")
        self.assertEqual(metrics.pothole_count, 1)
        self.assertEqual(metrics.weather_condition, "dry")
        self.assertEqual(metrics.model_name, "YOLOv8RoadQuality")

    def test_to_dict_conversion(self):
        """Test conversion of metrics to dictionary format."""
        metrics_dict = self.sample_metrics.to_dict()

        # Check structure
        self.assertIn("overall_quality_score", metrics_dict)
        self.assertIn("crack_detection", metrics_dict)
        self.assertIn("pothole_detection", metrics_dict)
        self.assertIn("metadata", metrics_dict)

        # Check values
        self.assertEqual(metrics_dict["overall_quality_score"], 75.5)
        self.assertEqual(metrics_dict["crack_detection"]["confidence"], 0.8)
        self.assertEqual(metrics_dict["crack_detection"]["severity"], "moderate")
        self.assertEqual(metrics_dict["pothole_detection"]["count"], 1)
        self.assertEqual(metrics_dict["metadata"]["model_name"], "YOLOv8RoadQuality")

    def test_numpy_conversion(self):
        """Test proper conversion of numpy types to Python types."""
        metrics = RoadQualityMetrics(
            overall_quality_score=np.float64(75.5),
            crack_confidence=np.float32(0.8),
            crack_severity="moderate",
            pothole_confidence=np.float32(0.2),
            pothole_count=np.int32(1),
            surface_roughness=np.float64(0.3),
            lane_marking_visibility=np.float32(0.9),
            debris_score=np.float64(0.1),
            weather_condition="dry",
            assessment_confidence=np.float32(0.85),
            timestamp="2024-01-01T12:00:00Z",
            model_name="YOLOv8RoadQuality",
            model_version="1.0.0",
        )

        metrics_dict = metrics.to_dict()

        # All numeric values should be native Python types
        self.assertIsInstance(metrics_dict["overall_quality_score"], float)
        self.assertIsInstance(metrics_dict["crack_detection"]["confidence"], float)
        self.assertIsInstance(metrics_dict["pothole_detection"]["count"], int)

    def test_from_model_output(self):
        """Test creation of metrics from model output."""
        mock_predictions = {
            "crack_confidence": 0.8,
            "pothole_confidence": 0.6,
            "surface_roughness": 0.2,
            "lane_visibility": 0.9,
            "debris_score": 0.1,
            "confidence": 0.85,
        }

        mock_model_info = {"model_type": "YOLOv8RoadQuality", "version": "1.0.0"}

        metrics = RoadQualityMetrics.from_model_output(mock_predictions, mock_model_info)

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.crack_confidence, 0.8)
        self.assertEqual(metrics.pothole_confidence, 0.6)
        self.assertEqual(metrics.model_name, "YOLOv8RoadQuality")
        self.assertIsNotNone(metrics.timestamp)


class TestImagePreprocessor(unittest.TestCase):
    """Test image preprocessing functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.preprocessor = ImagePreprocessor()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def create_test_image(self, filename="test.jpg", size=(640, 480)):
        """Create a test image file."""
        image_path = Path(self.temp_dir) / filename
        image = Image.new("RGB", size, color="red")
        image.save(image_path)
        return str(image_path)

    def test_load_and_preprocess_valid_image(self):
        """Test loading and preprocessing a valid image."""
        image_path = self.create_test_image()

        result = self.preprocessor.load_and_preprocess(image_path)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, np.ndarray)

    def test_load_nonexistent_image(self):
        """Test handling of non-existent image files."""
        nonexistent_path = "/path/to/nonexistent/image.jpg"

        result = self.preprocessor.load_and_preprocess(nonexistent_path)

        self.assertIsNone(result)

    def test_load_invalid_image_format(self):
        """Test handling of invalid image formats."""
        # Create a text file with image extension
        invalid_path = Path(self.temp_dir) / "invalid.jpg"
        with open(invalid_path, "w") as f:
            f.write("This is not an image")

        result = self.preprocessor.load_and_preprocess(str(invalid_path))

        self.assertIsNone(result)

    def test_preprocessor_initialization(self):
        """Test preprocessor initialization."""
        self.assertIsNotNone(self.preprocessor)
        self.assertIsInstance(self.preprocessor, ImagePreprocessor)


class TestModelFactory(unittest.TestCase):
    """Test model factory functionality."""

    def test_create_default_model(self):
        """Test creation of default YOLO model."""
        model = ModelFactory.create_model()

        self.assertIsNotNone(model)
        self.assertIsInstance(model, YOLOv8RoadModel)

    def test_create_model_with_custom_path(self):
        """Test creation of model with custom path."""
        custom_path = "/custom/model/path.pt"

        model = ModelFactory.create_model(custom_path)

        self.assertIsNotNone(model)
        self.assertIsInstance(model, YOLOv8RoadModel)

    def test_factory_functionality(self):
        """Test model factory basic functionality."""
        # Test that factory can create models
        model1 = ModelFactory.create_model()
        model2 = ModelFactory.create_model("/custom/path.pt")

        self.assertIsNotNone(model1)
        self.assertIsNotNone(model2)


class TestYOLOv8RoadModel(unittest.TestCase):
    """Test YOLO model implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.model = YOLOv8RoadModel()

    @patch("ultralytics.YOLO")
    def test_model_loading_success(self, mock_yolo):
        """Test successful model loading."""
        mock_yolo_instance = Mock()
        mock_yolo.return_value = mock_yolo_instance

        success = self.model.load_model()

        self.assertTrue(success)
        self.assertTrue(self.model.is_loaded)

    @patch("ultralytics.YOLO")
    def test_model_loading_failure(self, mock_yolo):
        """Test model loading failure handling."""
        mock_yolo.side_effect = Exception("Model loading failed")
        
        with self.assertRaises(RuntimeError) as context:
            self.model.load_model()

        self.assertIn("Failed to load YOLOv8 model", str(context.exception))
        self.assertFalse(self.model.is_loaded)

    def test_predict_without_loaded_model(self):
        """Test prediction without loaded model."""
        image_array = np.zeros((640, 480, 3), dtype=np.uint8)

        with self.assertRaises(RuntimeError):
            self.model.predict(image_array)

    @patch("ultralytics.YOLO")
    def test_predict_with_loaded_model(self, mock_yolo):
        """Test prediction with loaded model."""
        # Setup mock
        mock_yolo_instance = Mock()
        mock_results = Mock()

        # Mock individual box objects
        mock_box = Mock()
        mock_box.cls = [0]  # crack class
        mock_box.conf = [0.8]  # confidence

        # Mock boxes as an iterable containing our mock box
        mock_results.boxes = [mock_box]
        mock_yolo_instance.return_value = [mock_results]
        mock_yolo.return_value = mock_yolo_instance

        # Load model and predict
        self.model.load_model()
        image_array = np.zeros((640, 480, 3), dtype=np.uint8)

        predictions = self.model.predict(image_array)

        self.assertIsNotNone(predictions)
        self.assertIsInstance(predictions, dict)

    def test_get_model_info(self):
        """Test model information retrieval."""
        info = self.model.get_model_info()

        self.assertIsInstance(info, dict)
        self.assertIn("model_type", info)
        self.assertIn("version", info)
        self.assertIn("model_type", info)


class TestRoadQualityService(unittest.TestCase):
    """Test main RoadQualityService functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def create_test_image(self, filename="test.jpg", size=(640, 480)):
        """Create a test image file."""
        image_path = Path(self.temp_dir) / filename
        image = Image.new("RGB", size, color="blue")
        image.save(image_path)
        return str(image_path)

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    def test_service_initialization_success(self, mock_factory):
        """Test successful service initialization."""
        mock_model = Mock()
        mock_model.load_model.return_value = True
        mock_factory.create_model.return_value = mock_model

        service = RoadQualityService()

        self.assertIsNotNone(service)
        mock_model.load_model.assert_called_once()

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    def test_service_initialization_failure(self, mock_factory):
        """Test service initialization failure."""
        mock_model = Mock()
        mock_model.load_model.return_value = False
        mock_factory.create_model.return_value = mock_model

        with self.assertRaises(RuntimeError):
            RoadQualityService()

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    @patch("src.services.road_quality.road_quality_service.ImagePreprocessor")
    def test_assess_road_quality_success(self, mock_preprocessor_class, mock_factory):
        """Test successful road quality assessment."""
        # Setup mocks
        mock_model = Mock()
        mock_model.load_model.return_value = True
        mock_model.predict.return_value = {"quality_score": 75.0, "detections": []}
        mock_model.get_model_info.return_value = {"model_name": "YOLOv8", "version": "1.0.0"}
        mock_factory.create_model.return_value = mock_model

        mock_preprocessor = Mock()
        mock_preprocessor.load_and_preprocess.return_value = np.zeros((640, 480, 3))
        mock_preprocessor_class.return_value = mock_preprocessor

        # Create test image and service
        image_path = self.create_test_image()
        service = RoadQualityService()

        # Mock the metrics creation
        with patch(
            "src.services.road_quality.road_quality_service.RoadQualityMetrics"
        ) as mock_metrics:
            mock_metrics.from_model_output.return_value = Mock(spec=RoadQualityMetrics)

            result = service.assess_road_quality(image_path)

            self.assertIsNotNone(result)
            mock_preprocessor.load_and_preprocess.assert_called_once_with(image_path)
            mock_model.predict.assert_called_once()

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    def test_assess_road_quality_nonexistent_file(self, mock_factory):
        """Test assessment with non-existent image file."""
        mock_model = Mock()
        mock_model.load_model.return_value = True
        mock_factory.create_model.return_value = mock_model

        service = RoadQualityService()

        with self.assertRaises(RuntimeError):
            service.assess_road_quality("/nonexistent/image.jpg")

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    @patch("src.services.road_quality.road_quality_service.ImagePreprocessor")
    def test_assess_road_quality_preprocessing_failure(self, mock_preprocessor_class, mock_factory):
        """Test assessment when preprocessing fails."""
        # Setup mocks
        mock_model = Mock()
        mock_model.load_model.return_value = True
        mock_factory.create_model.return_value = mock_model

        mock_preprocessor = Mock()
        mock_preprocessor.load_and_preprocess.return_value = None  # Preprocessing failed
        mock_preprocessor_class.return_value = mock_preprocessor

        # Create test image and service
        image_path = self.create_test_image()
        service = RoadQualityService()

        result = service.assess_road_quality(image_path)

        self.assertIsNone(result)

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    def test_get_service_info(self, mock_factory):
        """Test service information retrieval."""
        mock_model = Mock()
        mock_model.load_model.return_value = True
        mock_model.get_model_info.return_value = {
            "model_type": "YOLOv8RoadQuality",
            "version": "1.0.0",
        }
        mock_factory.create_model.return_value = mock_model

        service = RoadQualityService()
        info = service.get_service_info()

        self.assertIsInstance(info, dict)
        self.assertIn("service_version", info)
        self.assertIn("model_info", info)

    @patch("src.services.road_quality.road_quality_service.ModelFactory")
    @patch("src.services.road_quality.road_quality_service.ImagePreprocessor")
    def test_batch_assessment(self, mock_preprocessor_class, mock_factory):
        """Test batch processing of multiple images."""
        # Setup mocks
        mock_model = Mock()
        mock_model.load_model.return_value = True
        mock_model.predict.return_value = {"quality_score": 75.0, "detections": []}
        mock_model.get_model_info.return_value = {"model_name": "YOLOv8", "version": "1.0.0"}
        mock_factory.create_model.return_value = mock_model

        mock_preprocessor = Mock()
        mock_preprocessor.load_and_preprocess.return_value = np.zeros((640, 480, 3))
        mock_preprocessor_class.return_value = mock_preprocessor

        # Create test images
        image_paths = [
            self.create_test_image("test1.jpg"),
            self.create_test_image("test2.jpg"),
            self.create_test_image("test3.jpg"),
        ]

        service = RoadQualityService()

        # Mock the metrics creation
        with patch(
            "src.services.road_quality.road_quality_service.RoadQualityMetrics"
        ) as mock_metrics:
            mock_metrics.from_model_output.return_value = Mock(spec=RoadQualityMetrics)

            results = service.batch_assess(image_paths)

            self.assertEqual(len(results), 3)
            for path in image_paths:
                self.assertIn(path, results)
                self.assertIsNotNone(results[path])


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete road quality assessment pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def create_test_image(self, filename="test.jpg", size=(640, 480)):
        """Create a test image file."""
        image_path = Path(self.temp_dir) / filename
        image = Image.new("RGB", size, color="green")
        image.save(image_path)
        return str(image_path)

    @patch("ultralytics.YOLO")
    def test_end_to_end_assessment(self, mock_yolo):
        """Test complete end-to-end road quality assessment."""
        # Setup mock YOLO model
        mock_yolo_instance = Mock()
        mock_results = Mock()

        # Mock individual box objects
        mock_box1 = Mock()
        mock_box1.cls = [0]  # crack class
        mock_box1.conf = [0.8]  # confidence

        mock_box2 = Mock()
        mock_box2.cls = [1]  # pothole class
        mock_box2.conf = [0.6]  # confidence

        # Mock boxes as an iterable containing our mock boxes
        mock_results.boxes = [mock_box1, mock_box2]
        mock_yolo_instance.return_value = [mock_results]
        mock_yolo.return_value = mock_yolo_instance

        # Create test image
        image_path = self.create_test_image()

        # Run assessment
        service = RoadQualityService()

        with patch(
            "src.services.road_quality.metrics.RoadQualityMetrics.from_model_output"
        ) as mock_from_output:
            expected_metrics = RoadQualityMetrics(
                overall_quality_score=75.0,
                crack_confidence=0.8,
                crack_severity="moderate",
                pothole_confidence=0.6,
                pothole_count=1,
                surface_roughness=0.3,
                lane_marking_visibility=0.9,
                debris_score=0.1,
                weather_condition="dry",
                assessment_confidence=0.7,
                timestamp="2024-01-01T12:00:00Z",
                model_name="YOLOv8RoadQuality",
                model_version="1.0.0",
            )
            mock_from_output.return_value = expected_metrics

            result = service.assess_road_quality(image_path)

            self.assertIsNotNone(result)
            self.assertEqual(result.overall_quality_score, 75.0)
            self.assertEqual(result.crack_severity, "moderate")
            self.assertEqual(result.pothole_count, 1)


def run_tests():
    """Run all tests with detailed output."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    test_classes = [
        TestRoadQualityMetrics,
        TestImagePreprocessor,
        TestModelFactory,
        TestYOLOv8RoadModel,
        TestRoadQualityService,
        TestIntegration,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print(f"\n{'=' * 50}")
    print("Test Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(
        f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%"
    )

    return result.wasSuccessful()


if __name__ == "__main__":
    print("🧪 Running Road Quality Service Unit Tests")
    print("=" * 50)
    success = run_tests()
    exit(0 if success else 1)
