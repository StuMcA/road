"""
Database service for Road Quality Analysis system.

Handles all database operations including:
- Photo metadata storage
- Quality assessment results
- Road analysis results
- Duplicate detection
- Transaction management
"""

import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from ...services.image.quality.quality_metrics import ImageQualityMetrics
from ...services.road.analysis.metrics import RoadQualityMetrics


load_dotenv()
logger = logging.getLogger(__name__)


class DatabaseService:
    """Handles all database operations for road quality analysis."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """Initialize database service with connection parameters."""

        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = port or int(os.getenv("DB_PORT", 5432))
        self.database = database or os.getenv("DB_NAME", "road_quality")
        self.user = user or os.getenv("DB_USER", "postgres")
        self.password = password or os.getenv("DB_PASSWORD")

        if not self.password:
            raise ValueError(
                "Database password must be provided via DB_PASSWORD env var or constructor"
            )

    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            cursor_factory=RealDictCursor,
        )

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        conn = None
        try:
            conn = self.get_connection()
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            if conn:
                conn.rollback()
                logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def check_duplicate_photo(
        self,
        source: str,
        source_image_id: str = None,
        location: tuple[float, float] = None,
        date_taken: datetime = None,
    ) -> Optional[dict[str, Any]]:
        """
        Check if a photo already exists in the database.

        Args:
            source: Image source ('mapillary', 'streetview', etc.)
            source_image_id: Original platform image ID
            location: (latitude, longitude) tuple
            date_taken: When photo was captured

        Returns:
            Photo record dict if duplicate found, None otherwise
        """
        with self.transaction() as conn:
            cursor = conn.cursor()

            # Primary: Check by source + source_image_id
            if source_image_id:
                cursor.execute(
                    """
                    SELECT id, source, source_image_id,
                           ST_Y(location) as latitude, ST_X(location) as longitude,
                           date_taken, created_at
                    FROM photos
                    WHERE source = %s AND source_image_id = %s
                """,
                    (source, source_image_id),
                )

                result = cursor.fetchone()
                if result:
                    logger.info(
                        f"Found duplicate photo by source_image_id: {source}:{source_image_id}"
                    )
                    return dict(result)

            # Secondary: Check by exact location + date_taken
            if location and date_taken:
                lat, lon = location
                cursor.execute(
                    """
                    SELECT id, source, source_image_id,
                           ST_Y(location) as latitude, ST_X(location) as longitude,
                           date_taken, created_at
                    FROM photos
                    WHERE ST_Equals(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                      AND date_taken = %s
                """,
                    (lon, lat, date_taken),
                )

                result = cursor.fetchone()
                if result:
                    logger.info(
                        f"Found duplicate photo by location+time: {lat},{lon} at {date_taken}"
                    )
                    return dict(result)

            return None

    def save_photo(
        self,
        source: str,
        source_image_id: str = None,
        location: tuple[float, float] = None,
        date_taken: datetime = None,
        compass_angle: float = None,
        street_point_id: int = None,
        street_data_id: int = None,
    ) -> int:
        """
        Save photo metadata to database.

        Args:
            source: Image source ('mapillary', 'streetview', etc.)
            source_image_id: Original platform image ID
            location: (latitude, longitude) tuple
            date_taken: When photo was captured
            compass_angle: Camera direction in degrees (0-360)
            street_point_id: Reference to street point (nullable in Phase 1)
            street_data_id: Reference to street data entry (TOID-based)

        Returns:
            Photo ID
        """
        with self.transaction() as conn:
            cursor = conn.cursor()

            # Create PostGIS point from lat/lon with SRID 4326
            location_sql = None
            if location:
                lat, lon = location
                location_sql = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"

            cursor.execute(
                f"""
                INSERT INTO photos (
                    street_point_id, street_data_id, source, source_image_id,
                    location, date_taken, compass_angle
                ) VALUES (
                    %s, %s, %s, %s,
                    {location_sql if location_sql else "NULL"}, %s, %s
                ) RETURNING id
            """,
                (street_point_id, street_data_id, source, source_image_id, date_taken, compass_angle),
            )

            result = cursor.fetchone()
            photo_id = result["id"] if result else None
            if not photo_id:
                raise Exception("Failed to get photo ID from insert")
            logger.info(f"Saved photo {photo_id}: {source}:{source_image_id}")
            return photo_id

    def save_quality_result(self, photo_id: int, quality_metrics: ImageQualityMetrics) -> int:
        """
        Save quality assessment results to database.

        Args:
            photo_id: Reference to photo
            quality_metrics: Quality assessment results

        Returns:
            Quality result ID
        """
        with self.transaction() as conn:
            cursor = conn.cursor()

            # Convert numpy types to Python native types for database compatibility
            def convert_numpy(val):
                """Convert numpy types to native Python types"""
                import numpy as np
                if isinstance(val, (np.integer, np.floating)):
                    return val.item()
                return val

            # Convert failure reasons enum to strings
            failure_reasons = (
                [reason.value for reason in quality_metrics.failure_reasons]
                if quality_metrics.failure_reasons
                else []
            )

            cursor.execute(
                """
                INSERT INTO quality_results (
                    photo_id, overall_score, blur_score, exposure_score, size_score,
                    road_surface_percentage, has_sufficient_road, is_usable,
                    failure_reasons, assessment_version
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
            """,
                (
                    photo_id,
                    convert_numpy(quality_metrics.overall_score),
                    convert_numpy(quality_metrics.blur_score),
                    convert_numpy(quality_metrics.exposure_score),
                    convert_numpy(quality_metrics.size_score),
                    convert_numpy(quality_metrics.road_surface_percentage),
                    quality_metrics.has_sufficient_road,
                    quality_metrics.is_usable,
                    failure_reasons,
                    getattr(quality_metrics, "assessment_version", "1.0.0"),
                ),
            )

            result = cursor.fetchone()
            quality_id = result["id"] if result else None
            if not quality_id:
                raise Exception("Failed to get quality result ID from insert")
            logger.info(
                f"Saved quality result {quality_id} for photo {photo_id}: usable={quality_metrics.is_usable}"
            )
            return quality_id

    def save_road_analysis_result(self, photo_id: int, road_metrics: RoadQualityMetrics) -> int:
        """
        Save road analysis results to database.

        Args:
            photo_id: Reference to photo
            road_metrics: Road analysis results

        Returns:
            Road analysis result ID
        """
        with self.transaction() as conn:
            cursor = conn.cursor()

            # Map crack severity to enum
            crack_severity_map = {
                "none": "none",
                "minor": "minor",
                "moderate": "moderate",
                "severe": "severe",
            }
            crack_severity = crack_severity_map.get(road_metrics.crack_severity, "none")

            # Convert numpy types to Python native types for database compatibility
            def convert_numpy(val):
                """Convert numpy types to native Python types"""
                import numpy as np
                if isinstance(val, (np.integer, np.floating)):
                    return val.item()
                return val

            # Map surface type (if available in road_metrics)
            surface_type = getattr(road_metrics, "surface_type", None)

            # Determine quality rating from overall score
            score = convert_numpy(road_metrics.overall_quality_score)
            if score >= 90:
                quality_rating = "excellent"
            elif score >= 75:
                quality_rating = "good"
            elif score >= 50:
                quality_rating = "fair"
            elif score >= 25:
                quality_rating = "poor"
            else:
                quality_rating = "severe_issues"

            cursor.execute(
                """
                INSERT INTO road_analysis_results (
                    photo_id, overall_quality_score, quality_rating,
                    crack_confidence, crack_severity, pothole_confidence, pothole_count,
                    surface_roughness, surface_type, lane_marking_visibility, debris_score,
                    weather_condition, assessment_confidence,
                    model_name, model_version
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
            """,
                (
                    photo_id,
                    convert_numpy(road_metrics.overall_quality_score),
                    quality_rating,
                    convert_numpy(road_metrics.crack_confidence),
                    crack_severity,
                    convert_numpy(road_metrics.pothole_confidence),
                    convert_numpy(road_metrics.pothole_count),
                    convert_numpy(road_metrics.surface_roughness),
                    convert_numpy(road_metrics.lane_marking_visibility),
                    convert_numpy(road_metrics.debris_score),
                    surface_type,
                    road_metrics.weather_condition,
                    convert_numpy(road_metrics.assessment_confidence),
                    road_metrics.model_name,
                    road_metrics.model_version,
                ),
            )

            result = cursor.fetchone()
            analysis_id = result["id"] if result else None
            if not analysis_id:
                raise Exception("Failed to get road analysis ID from insert")
            logger.info(
                f"Saved road analysis {analysis_id} for photo {photo_id}: score={road_metrics.overall_quality_score:.1f}"
            )
            return analysis_id

    def get_photo_with_results(self, photo_id: int) -> Optional[dict[str, Any]]:
        """
        Get photo with its quality and road analysis results.

        Args:
            photo_id: Photo ID to retrieve

        Returns:
            Complete photo record with results, or None if not found
        """
        with self.transaction() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    p.id as photo_id,
                    p.source,
                    p.source_image_id,
                    ST_Y(p.location) as latitude,
                    ST_X(p.location) as longitude,
                    p.date_taken,
                    p.compass_angle,
                    p.created_at as photo_created_at,

                    q.id as quality_id,
                    q.overall_score as quality_score,
                    q.is_usable,
                    q.failure_reasons,
                    q.date_calculated as quality_date,

                    r.id as road_analysis_id,
                    r.overall_quality_score as road_score,
                    r.quality_rating,
                    r.crack_confidence,
                    r.crack_severity,
                    r.pothole_count,
                    r.date_calculated as analysis_date

                FROM photos p
                LEFT JOIN quality_results q ON p.id = q.photo_id
                LEFT JOIN road_analysis_results r ON p.id = r.photo_id
                WHERE p.id = %s
            """,
                (photo_id,),
            )

            result = cursor.fetchone()
            return dict(result) if result else None

    def get_processing_stats(self) -> dict[str, Any]:
        """Get summary statistics about processed photos."""
        with self.transaction() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    COUNT(p.id) as total_photos,
                    COUNT(q.id) as quality_assessed,
                    COUNT(CASE WHEN q.is_usable THEN 1 END) as usable_photos,
                    COUNT(r.id) as road_analyzed,
                    AVG(q.overall_score) as avg_quality_score,
                    AVG(r.overall_quality_score) as avg_road_score,
                    MAX(q.date_calculated) as last_quality_assessment,
                    MAX(r.date_calculated) as last_road_analysis
                FROM photos p
                LEFT JOIN quality_results q ON p.id = q.photo_id
                LEFT JOIN road_analysis_results r ON p.id = r.photo_id
            """)

            result = cursor.fetchone()
            if result:
                return {
                    'total_photos': result['total_photos'] or 0,
                    'quality_assessed': result['quality_assessed'] or 0,
                    'usable_photos': result['usable_photos'] or 0,
                    'road_analyzed': result['road_analyzed'] or 0,
                    'avg_quality_score': float(result['avg_quality_score']) if result['avg_quality_score'] else None,
                    'avg_road_score': float(result['avg_road_score']) if result['avg_road_score'] else None,
                    'last_quality_assessment': result['last_quality_assessment'],
                    'last_road_analysis': result['last_road_analysis']
                }
            return {}
