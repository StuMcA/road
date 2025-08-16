from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from ..image_quality import ImageQualityMetrics
from ..road_quality import RoadQualityMetrics


@dataclass
class PipelineResult:
    """Complete pipeline result including quality check and road analysis"""

    image_path: str
    processed_successfully: bool

    # Quality assessment (always performed)
    quality_metrics: ImageQualityMetrics

    # Road analysis (only if quality check passes)
    road_metrics: Optional[RoadQualityMetrics]

    # Pipeline metadata
    timestamp: str
    processing_time_ms: float
    pipeline_version: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "image_path": self.image_path,
            "processed_successfully": self.processed_successfully,
            "quality_assessment": self.quality_metrics.to_dict(),
            "road_analysis": self.road_metrics.to_dict() if self.road_metrics else None,
            "pipeline_metadata": {
                "timestamp": self.timestamp,
                "processing_time_ms": self.processing_time_ms,
                "pipeline_version": self.pipeline_version,
            },
        }

    @property
    def summary(self) -> str:
        """Human-readable summary of pipeline result"""
        if not self.processed_successfully:
            reasons = ", ".join(
                [reason.display_message for reason in self.quality_metrics.failure_reasons]
            )
            return f"❌ Failed quality check: {reasons}"

        road_score = self.road_metrics.overall_quality_score if self.road_metrics else 0
        quality_score = self.quality_metrics.overall_score

        return f"✅ Quality: {quality_score:.1f}/100, Road: {road_score:.1f}/100"

    @classmethod
    def create_quality_failed(
        cls, image_path: str, quality_metrics: ImageQualityMetrics, processing_time_ms: float
    ) -> "PipelineResult":
        """Create result for images that failed quality check"""
        return cls(
            image_path=image_path,
            processed_successfully=False,
            quality_metrics=quality_metrics,
            road_metrics=None,
            timestamp=datetime.now().isoformat(),
            processing_time_ms=processing_time_ms,
            pipeline_version="1.0.0",
        )

    @classmethod
    def create_success(
        cls,
        image_path: str,
        quality_metrics: ImageQualityMetrics,
        road_metrics: RoadQualityMetrics,
        processing_time_ms: float,
    ) -> "PipelineResult":
        """Create result for successfully processed images"""
        return cls(
            image_path=image_path,
            processed_successfully=True,
            quality_metrics=quality_metrics,
            road_metrics=road_metrics,
            timestamp=datetime.now().isoformat(),
            processing_time_ms=processing_time_ms,
            pipeline_version="1.0.0",
        )
