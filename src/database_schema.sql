-- Road Quality Analysis Database Schema
-- PostgreSQL 13+ with PostGIS
-- Complete schema including Phase 1 (individual analysis) and Phase 2 (group analysis) support

-- Enable PostGIS extension for spatial data
CREATE EXTENSION IF NOT EXISTS postgis;

-- Custom enum types
CREATE TYPE road_surface_type AS ENUM (
    'asphalt',
    'concrete', 
    'cobblestone',
    'gravel',
    'dirt',
    'brick',
    'other'
);

CREATE TYPE road_quality_rating AS ENUM (
    'excellent',
    'good', 
    'fair',
    'poor',
    'severe_issues'
);

CREATE TYPE accessibility_type AS ENUM (
    'all_vehicles',
    'cars_only',
    'bikes_pedestrians', 
    'pedestrians_only',
    'restricted',
    'unknown'
);

CREATE TYPE image_source AS ENUM (
    'mapillary',
    'streetview',
    'manual_upload',
    'other'
);

CREATE TYPE crack_severity AS ENUM (
    'none',
    'minor',
    'moderate', 
    'severe'
);

-- Streets table - administrative/geographic grouping
CREATE TABLE streets (
    id SERIAL PRIMARY KEY,
    street_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Street points - specific analysis locations along streets
CREATE TABLE street_points (
    id SERIAL PRIMARY KEY,
    street_id INTEGER REFERENCES streets(id) ON DELETE CASCADE,
    location GEOMETRY(POINT, 4326) NOT NULL, -- WGS84 lat/lon
    postcode VARCHAR(20),
    local_authority VARCHAR(255),
    region VARCHAR(255),
    description TEXT, -- Optional description of this point
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Photos - source images for analysis
CREATE TABLE photos (
    id SERIAL PRIMARY KEY,
    street_point_id INTEGER REFERENCES street_points(id) ON DELETE CASCADE, -- Nullable in Phase 1
    source image_source NOT NULL,
    source_image_id VARCHAR(255), -- Original platform ID (e.g., Mapillary image ID)
    location GEOMETRY(POINT, 4326), -- Exact camera position (may differ from street_point)
    date_taken TIMESTAMP WITH TIME ZONE,
    compass_angle FLOAT, -- Camera direction in degrees (0-360)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT photos_compass_angle_check CHECK (compass_angle >= 0 AND compass_angle < 360)
);

-- Quality assessment results (Phase 1: individual photo analysis)
CREATE TABLE quality_results (
    id SERIAL PRIMARY KEY,
    photo_id INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    
    -- Quality scores (0-100, higher = better)
    overall_score FLOAT NOT NULL CHECK (overall_score >= 0 AND overall_score <= 100),
    blur_score FLOAT NOT NULL CHECK (blur_score >= 0 AND blur_score <= 100),
    exposure_score FLOAT NOT NULL CHECK (exposure_score >= 0 AND exposure_score <= 100),
    size_score FLOAT NOT NULL CHECK (size_score >= 0 AND size_score <= 100),
    
    -- Road surface analysis
    road_surface_percentage FLOAT NOT NULL CHECK (road_surface_percentage >= 0 AND road_surface_percentage <= 100),
    has_sufficient_road BOOLEAN NOT NULL,
    
    -- Overall assessment
    is_usable BOOLEAN NOT NULL,
    failure_reasons TEXT[], -- Array of failure reason strings
    
    -- Processing metadata
    date_calculated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    assessment_version VARCHAR(50) NOT NULL
);

-- Road condition analysis results (Phase 1: individual photo analysis)
CREATE TABLE road_analysis_results (
    id SERIAL PRIMARY KEY,
    photo_id INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    
    -- Overall assessment
    overall_quality_score FLOAT NOT NULL CHECK (overall_quality_score >= 0 AND overall_quality_score <= 100),
    quality_rating road_quality_rating NOT NULL,
    
    -- Specific issue detection (0-1 confidence scores)
    crack_confidence FLOAT NOT NULL CHECK (crack_confidence >= 0 AND crack_confidence <= 1),
    crack_severity crack_severity NOT NULL,
    pothole_confidence FLOAT NOT NULL CHECK (pothole_confidence >= 0 AND pothole_confidence <= 1),
    pothole_count INTEGER NOT NULL CHECK (pothole_count >= 0),
    
    -- Surface characteristics
    surface_roughness FLOAT NOT NULL CHECK (surface_roughness >= 0 AND surface_roughness <= 1),
    surface_type road_surface_type,
    lane_marking_visibility FLOAT NOT NULL CHECK (lane_marking_visibility >= 0 AND lane_marking_visibility <= 1),
    debris_score FLOAT NOT NULL CHECK (debris_score >= 0 AND debris_score <= 1),
    
    -- Environmental factors
    weather_condition VARCHAR(50),
    
    -- Model confidence and metadata
    assessment_confidence FLOAT NOT NULL CHECK (assessment_confidence >= 0 AND assessment_confidence <= 1),
    date_calculated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL
);

-- Phase 2: Group analysis tables (ready for future multi-image analysis)
CREATE TABLE road_analysis_groups (
    id SERIAL PRIMARY KEY,
    location GEOMETRY(POINT, 4326) NOT NULL,
    analysis_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Group-level road analysis results (aggregated from multiple photos)
    overall_quality_score FLOAT NOT NULL CHECK (overall_quality_score >= 0 AND overall_quality_score <= 100),
    quality_rating road_quality_rating NOT NULL,
    
    -- Aggregated confidence scores
    avg_crack_confidence FLOAT NOT NULL CHECK (avg_crack_confidence >= 0 AND avg_crack_confidence <= 1),
    dominant_crack_severity crack_severity NOT NULL,
    avg_pothole_confidence FLOAT NOT NULL CHECK (avg_pothole_confidence >= 0 AND avg_pothole_confidence <= 1),
    total_pothole_count INTEGER NOT NULL CHECK (total_pothole_count >= 0),
    
    -- Surface characteristics
    avg_surface_roughness FLOAT NOT NULL CHECK (avg_surface_roughness >= 0 AND avg_surface_roughness <= 1),
    dominant_surface_type road_surface_type,
    avg_lane_marking_visibility FLOAT NOT NULL CHECK (avg_lane_marking_visibility >= 0 AND avg_lane_marking_visibility <= 1),
    avg_debris_score FLOAT NOT NULL CHECK (avg_debris_score >= 0 AND avg_debris_score <= 1),
    
    -- Group metadata
    photo_count INTEGER NOT NULL CHECK (photo_count > 0),
    spatial_tolerance_m FLOAT DEFAULT 5.0,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Link multiple quality-passed photos to one group analysis (Phase 2)
CREATE TABLE group_analysis_photos (
    group_analysis_id INTEGER REFERENCES road_analysis_groups(id) ON DELETE CASCADE,
    photo_id INTEGER REFERENCES photos(id) ON DELETE CASCADE,
    quality_result_id INTEGER REFERENCES quality_results(id) ON DELETE CASCADE,
    PRIMARY KEY (group_analysis_id, photo_id)
);

-- Ensure one-to-one relationships for individual analysis
ALTER TABLE quality_results ADD CONSTRAINT unique_quality_per_photo UNIQUE (photo_id);
ALTER TABLE road_analysis_results ADD CONSTRAINT unique_analysis_per_photo UNIQUE (photo_id);

-- Create indexes for performance
-- Streets indexes  
CREATE INDEX idx_streets_name ON streets(street_name);
CREATE INDEX idx_streets_created_at ON streets(created_at);

-- Street points indexes
CREATE INDEX idx_street_points_street_id ON street_points(street_id);
CREATE INDEX idx_street_points_location ON street_points USING GIST(location);
CREATE INDEX idx_street_points_postcode ON street_points(postcode);
CREATE INDEX idx_street_points_local_authority ON street_points(local_authority);

-- Photos indexes
CREATE INDEX idx_photos_street_point ON photos(street_point_id);
CREATE INDEX idx_photos_source ON photos(source);
CREATE INDEX idx_photos_source_image_id ON photos(source_image_id);
CREATE INDEX idx_photos_location ON photos USING GIST(location);
CREATE INDEX idx_photos_date_taken ON photos(date_taken);

-- Quality results indexes
CREATE INDEX idx_quality_results_photo ON quality_results(photo_id);
CREATE INDEX idx_quality_results_usable ON quality_results(is_usable);
CREATE INDEX idx_quality_results_date ON quality_results(date_calculated);

-- Individual road analysis indexes
CREATE INDEX idx_road_analysis_photo ON road_analysis_results(photo_id);
CREATE INDEX idx_road_analysis_quality_rating ON road_analysis_results(quality_rating);
CREATE INDEX idx_road_analysis_surface_type ON road_analysis_results(surface_type);
CREATE INDEX idx_road_analysis_date ON road_analysis_results(date_calculated);
CREATE INDEX idx_road_analysis_model ON road_analysis_results(model_name, model_version);

-- Group analysis indexes (Phase 2)
CREATE INDEX idx_road_analysis_groups_location ON road_analysis_groups USING GIST(location);
CREATE INDEX idx_road_analysis_groups_date ON road_analysis_groups(analysis_date);
CREATE INDEX idx_group_analysis_photos_group ON group_analysis_photos(group_analysis_id);
CREATE INDEX idx_group_analysis_photos_quality ON group_analysis_photos(quality_result_id);

-- Views for common queries

-- Complete photo analysis view (handles nullable street_point_id for Phase 1)
CREATE VIEW photo_analysis_summary AS
SELECT 
    p.id as photo_id,
    p.source,
    p.source_image_id,
    p.date_taken,
    p.compass_angle,
    ST_Y(p.location) as latitude,
    ST_X(p.location) as longitude,
    
    -- Street information (nullable in Phase 1)
    s.street_name,
    sp.postcode,
    sp.local_authority,
    
    -- Quality assessment
    q.overall_score as quality_score,
    q.is_usable,
    q.has_sufficient_road,
    
    -- Road analysis (may be null if quality failed)
    r.overall_quality_score as road_quality_score,
    r.quality_rating,
    r.surface_type,
    r.crack_severity,
    r.pothole_count,
    
    -- Processing dates
    q.date_calculated as quality_date,
    r.date_calculated as analysis_date
    
FROM photos p
LEFT JOIN street_points sp ON p.street_point_id = sp.id  -- LEFT JOIN for nullable FK
LEFT JOIN streets s ON sp.street_id = s.id
LEFT JOIN quality_results q ON p.id = q.photo_id
LEFT JOIN road_analysis_results r ON p.id = r.photo_id;

-- Comments for documentation
COMMENT ON TABLE streets IS 'Administrative grouping of road segments';
COMMENT ON TABLE street_points IS 'Specific geographic points along streets for analysis';
COMMENT ON TABLE photos IS 'Source images from various platforms';
COMMENT ON TABLE quality_results IS 'Image quality assessment results (1:1 with photos)';
COMMENT ON TABLE road_analysis_results IS 'Road condition analysis results (only for usable images)';
COMMENT ON TABLE road_analysis_groups IS 'Group-level road analysis combining multiple quality-passed photos (Phase 2 functionality)';
COMMENT ON TABLE group_analysis_photos IS 'Links quality-passed photos to group analyses (Phase 2 functionality)';

COMMENT ON COLUMN photos.street_point_id IS 'Reference to street point (nullable in Phase 1, will be populated when OS API integration is added)';
COMMENT ON COLUMN photos.compass_angle IS 'Camera direction in degrees (0=North, 90=East, 180=South, 270=West)';
COMMENT ON COLUMN quality_results.failure_reasons IS 'Array of reasons why image failed quality check';
COMMENT ON COLUMN road_analysis_results.surface_roughness IS 'Surface roughness score (0=smooth, 1=very rough)';
