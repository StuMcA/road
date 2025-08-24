# Road Quality Analysis Pipeline

A comprehensive system for analyzing road quality using street-level imagery from Mapillary API combined with Ordnance Survey geospatial data.

## Overview

This project implements a complete pipeline for:
1. **Data Collection**: Fetching street points and metadata from OS APIs
2. **Image Acquisition**: Retrieving street-level imagery from Mapillary API  
3. **Quality Assessment**: AI-powered image quality evaluation
4. **Road Analysis**: Deep learning-based road condition assessment
5. **Database Storage**: Persistent storage with PostGIS spatial capabilities

## Current Architecture

### Core Components

- **Street Data Service** (`src/services/street_data_service.py`)
  - Multithreaded OS API data collection
  - Batch processing with database saves
  - Rate limiting compliance (600 req/min)

- **Database Pipeline** (`src/services/pipeline/database_pipeline.py`)
  - Complete image processing workflow
  - Transaction-safe database operations
  - Duplicate detection and handling

- **Quality Assessment** (`src/services/image/quality/`)
  - Fast heuristic checks
  - AI-powered segmentation analysis
  - Quality gating for processing efficiency

- **Road Analysis** (`src/services/road/analysis/`)
  - YOLOv8-based road quality detection
  - Crack severity assessment
  - Structured metrics output

## Current Status

 **Completed Features:**
- OS Features API integration (bbox coordinate fix applied)
- OS Names API integration with field mapping
- Multithreaded street metadata collection
- Batch database saving architecture
- Complete image processing pipeline
- 8m precision radius implementation
- Sequential processing pipeline (49,312 points running)

= **In Progress:**
- Full dataset analysis (0.7% complete, ~23 hours ETA)
- High-precision image matching (8m radius)

## TODO List

### =¨ Priority: API Resilience and Error Handling

#### Mapillary API Improvements
- [ ] **429 Rate Limit Backoff Strategy**
  - Implement exponential backoff for 429 responses
  - Add jitter to prevent thundering herd
  - Configurable retry limits and delays
  - Log rate limit headers for monitoring

- [ ] **Robust Error Handling**
  - Network timeout recovery (currently 30s fixed)
  - Graceful handling of API outages
  - Circuit breaker pattern for repeated failures
  - Fallback mechanisms for critical operations

- [ ] **Request Resilience**
  - Retry logic for transient failures (5xx errors)
  - Request deduplication to prevent duplicate processing
  - Connection pooling optimization
  - Response validation and error recovery

#### Ordnance Survey API Improvements
- [ ] **Enhanced Error Recovery**
  - Handle quota exhaustion gracefully
  - Implement request queuing for rate limits
  - Add API health monitoring
  - Improve coordinate validation and error messages

### ¡ Performance Optimizations

#### Mapillary API Parallelisation
- [ ] **Thread-Safe Parallel Processing**
  - Fix database connection issues in multithreaded context
  - Implement proper connection pooling
  - Add thread-local pipeline instances
  - Coordinate rate limiting across threads

- [ ] **Advanced Parallelization Strategies**
  - Geographic partitioning for parallel processing
  - Batch optimization for API efficiency
  - Memory management for large datasets
  - Progress tracking across parallel workers

- [ ] **Caching and Optimization**
  - Implement Redis cache for API responses
  - Add image metadata caching
  - Optimize database query patterns
  - Implement smart retry with cached results

### =' Infrastructure Improvements

#### Monitoring and Observability
- [ ] **Comprehensive Logging**
  - Structured logging with correlation IDs
  - API response time metrics
  - Error rate monitoring per endpoint
  - Resource usage tracking

- [ ] **Health Checks and Alerts**
  - API availability monitoring
  - Database connection health checks
  - Processing pipeline status dashboard
  - Automated alerting for failures

#### Configuration Management
- [ ] **Environment-Based Configuration**
  - Separate configs for dev/staging/prod
  - API rate limit configuration per environment
  - Database connection pool sizing
  - Retry policy customization

### =Ê Data Quality and Validation

#### Enhanced Data Processing
- [ ] **Image Quality Improvements**
  - Additional quality heuristics
  - Machine learning model updates
  - Quality score calibration
  - False positive reduction

- [ ] **Road Analysis Enhancements**
  - Multi-model ensemble for accuracy
  - Temporal analysis for road condition trends
  - Weather condition impact assessment
  - Seasonal variation handling

### = Pipeline Optimizations

#### Processing Efficiency
- [ ] **Smart Batching**
  - Dynamic batch sizing based on API performance
  - Priority queuing for high-value areas
  - Checkpoint/resume functionality for long runs
  - Memory-efficient streaming processing

- [ ] **Geographic Optimization**
  - Spatial indexing for efficient coordinate queries
  - Region-based processing prioritization
  - Urban vs rural processing strategies
  - Coverage gap analysis and filling

## Current Pipeline Performance

- **Processing Rate**: ~2.9 points/second
- **API Compliance**: 600 requests/minute (OS APIs)
- **Precision**: 8m radius for high accuracy
- **Success Rate**: <1% expected (but highly accurate matches)
- **Database**: Transaction-safe with batch commits

## Getting Started

### Prerequisites
- Python 3.11+
- PostgreSQL with PostGIS
- OS_API_KEY environment variable
- MAPILLARY_ACCESS_TOKEN environment variable

### Installation
```bash
pip install -r requirements.txt
```

### Running the Pipeline
```bash
# Full dataset analysis (high precision)
python run_full_dataset_analysis.py

# Targeted analysis (specific areas)
python run_targeted_analysis.py

# Street data collection only
python -c "from src.services.street_data_service import StreetDataService; service = StreetDataService(); service.collect_street_data(bbox)"
```

## Database Schema

- **streets**: Street segment metadata
- **street_points**: Individual coordinate points with OS data
- **photos**: Mapillary image metadata with spatial links
- **quality_results**: Image quality assessment results  
- **road_analysis_results**: Road condition analysis outputs

## API Integration

### Ordnance Survey APIs
- **Features API**: TOID data collection (fixed coordinate ordering)
- **Names API**: Street metadata enrichment (field mapping implemented)

### Mapillary API
- **Images API**: Street-level imagery acquisition
- **Current Rate Limit**: Respecting fair use guidelines
- **Precision**: 8m radius for location accuracy

## Contributing

1. Focus on the TODO items above, prioritizing API resilience
2. Maintain 8m precision requirement for accuracy
3. Ensure database transaction safety
4. Add comprehensive error handling
5. Test with rate-limited APIs

## Architecture Notes

The system prioritizes accuracy over coverage, using an 8m radius to ensure road quality assessments are highly representative of specific locations rather than approximate regional conditions.