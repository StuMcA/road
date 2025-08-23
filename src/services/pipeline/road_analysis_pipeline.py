import time
from typing import Dict, List, Optional
from pathlib import Path

from ..image_quality import ImageQualityService
from ..road_quality import RoadQualityService
from ..image_fetcher import ImageFetcherService
from .pipeline_result import PipelineResult


class RoadAnalysisPipeline:
    """
    Complete pipeline for road quality analysis with quality gating
    
    Flow:
    1. [Optional] Image Fetching (from coordinates via Mapillary API)
    2. Image Quality Check (fast heuristics + AI segmentation)
    3. Quality Gate (only proceed if image is usable)
    4. Road Quality Analysis (expensive AI analysis)
    5. Return combined results
    """
    
    def __init__(self, road_model_path: Optional[str] = None, enable_fetcher: bool = True):
        """
        Initialize pipeline services
        
        Args:
            road_model_path: Optional path to custom road quality model
            enable_fetcher: Whether to initialize image fetcher service
        """
        self.quality_service = ImageQualityService()
        self.road_service = RoadQualityService(road_model_path)
        self.fetcher_service = ImageFetcherService() if enable_fetcher else None
        self.version = "1.1.0"
    
    def process_image(self, image_path: str) -> PipelineResult:
        """
        Process single image through complete pipeline
        
        Args:
            image_path: Path to image file
            
        Returns:
            PipelineResult with quality and road analysis
        """
        start_time = time.time()
        
        try:
            # Stage 1: Image Quality Assessment (always performed)
            quality_metrics = self.quality_service.evaluate(image_path)
            
            # Stage 2: Quality Gate
            if not quality_metrics.is_usable:
                # Image failed quality check - early termination
                processing_time = (time.time() - start_time) * 1000
                return PipelineResult.create_quality_failed(
                    image_path, quality_metrics, processing_time
                )
            
            # Stage 3: Road Quality Analysis (only for good images)
            road_metrics = self.road_service.assess_road_quality(image_path)
            
            if road_metrics is None:
                # Road analysis failed - treat as quality failure
                from ..image_quality import ImageFailureReason, ImageQualityMetrics
                failed_quality = ImageQualityMetrics.create_failed(
                    image_path, ImageFailureReason.PROCESSING_ERROR
                )
                processing_time = (time.time() - start_time) * 1000
                return PipelineResult.create_quality_failed(
                    image_path, failed_quality, processing_time
                )
            
            # Success: Both quality and road analysis completed
            processing_time = (time.time() - start_time) * 1000
            return PipelineResult.create_success(
                image_path, quality_metrics, road_metrics, processing_time
            )
            
        except Exception as e:
            # Pipeline error - return failure result
            from ..image_quality import ImageFailureReason, ImageQualityMetrics
            failed_quality = ImageQualityMetrics.create_failed(
                image_path, ImageFailureReason.PROCESSING_ERROR
            )
            processing_time = (time.time() - start_time) * 1000
            return PipelineResult.create_quality_failed(
                image_path, failed_quality, processing_time
            )
    
    def process_batch(self, image_paths: List[str]) -> Dict[str, PipelineResult]:
        """
        Process multiple images through pipeline
        
        Args:
            image_paths: List of image file paths
            
        Returns:
            Dictionary mapping image paths to pipeline results
        """
        results = {}
        for image_path in image_paths:
            results[image_path] = self.process_image(image_path)
        return results
    
    def process_coordinate(
        self,
        lat: float,
        lon: float,
        radius_m: Optional[float] = None,
        limit: int = 10,
        output_dir: Optional[str] = None
    ) -> Dict:
        """
        Fetch images at coordinates and process through complete pipeline
        
        Args:
            lat: Latitude in degrees
            lon: Longitude in degrees
            radius_m: Radius in meters for image search
            limit: Maximum number of images to fetch
            output_dir: Directory to download images
            
        Returns:
            Complete results including fetch info and pipeline analysis
            
        Raises:
            ValueError: If fetcher service is not enabled
        """
        if self.fetcher_service is None:
            raise ValueError("Image fetcher service not enabled. Initialize with enable_fetcher=True")
        
        return self.fetcher_service.fetch_and_process_images(
            lat, lon, self, radius_m, limit, output_dir
        )
    
    def get_pipeline_stats(self, results: Dict[str, PipelineResult]) -> Dict[str, any]:
        """
        Generate statistics from batch processing results
        
        Args:
            results: Dictionary of pipeline results
            
        Returns:
            Statistics summary
        """
        total_images = len(results)
        successful = sum(1 for r in results.values() if r.processed_successfully)
        failed_quality = sum(1 for r in results.values() if not r.processed_successfully)
        
        # Average processing times
        all_times = [r.processing_time_ms for r in results.values()]
        successful_times = [r.processing_time_ms for r in results.values() if r.processed_successfully]
        failed_times = [r.processing_time_ms for r in results.values() if not r.processed_successfully]
        
        # Quality scores for successful images
        quality_scores = [
            r.quality_metrics.overall_score for r in results.values() 
            if r.processed_successfully
        ]
        
        # Road scores for successful images  
        road_scores = [
            r.road_metrics.overall_quality_score for r in results.values()
            if r.processed_successfully and r.road_metrics
        ]
        
        return {
            "total_images": total_images,
            "successful_analyses": successful,
            "failed_quality_check": failed_quality,
            "success_rate": (successful / total_images * 100) if total_images > 0 else 0,
            "processing_times": {
                "average_total_ms": sum(all_times) / len(all_times) if all_times else 0,
                "average_successful_ms": sum(successful_times) / len(successful_times) if successful_times else 0,
                "average_failed_ms": sum(failed_times) / len(failed_times) if failed_times else 0
            },
            "quality_scores": {
                "average": sum(quality_scores) / len(quality_scores) if quality_scores else 0,
                "min": min(quality_scores) if quality_scores else 0,
                "max": max(quality_scores) if quality_scores else 0
            },
            "road_scores": {
                "average": sum(road_scores) / len(road_scores) if road_scores else 0,
                "min": min(road_scores) if road_scores else 0,
                "max": max(road_scores) if road_scores else 0
            }
        }
    
    def get_service_info(self) -> Dict[str, any]:
        """Get information about pipeline components"""
        info = {
            "pipeline_version": self.version,
            "quality_service": {
                "version": self.quality_service.version,
                "segmentation_available": self.quality_service.segmentation.model_loaded
            },
            "road_service": self.road_service.get_service_info(),
            "fetcher_service": None
        }
        
        if self.fetcher_service is not None:
            info["fetcher_service"] = self.fetcher_service.get_service_info()
            
        return info
