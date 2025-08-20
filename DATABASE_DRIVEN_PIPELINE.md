# Database-Driven TOID Analysis Pipeline

## Overview

The `TOIDAnalysisPipeline` has been updated to be purely database-driven and separated from external API clients. It now operates in two distinct phases:

1. **Data Collection Phase**: Use `StreetDataService` to populate database with street data from OS APIs
2. **Analysis Phase**: Use `TOIDAnalysisPipeline` to process existing data (database-only, no API calls)

## Key Changes

### Separation of Concerns
- **StreetDataService**: Handles all external API calls (OS Features API, OS Names API)
- **TOIDAnalysisPipeline**: Pure image analysis, reads only from database

### Graceful Failure Handling
- Pipeline fails gracefully when street data is not available
- Clear error messages and recommendations provided
- Distinguishes between missing data and system errors

### Database-Only Operation
- No external API dependencies during analysis
- All data must be pre-populated in database
- Improved reliability and performance

## Usage Pattern

### 1. Data Collection (One-time or periodic)
```python
from services.street_data_service import StreetDataService

# Collect and store street data
street_service = StreetDataService()
result = street_service.collect_street_data(
    bbox=area_bbox,
    max_features=100
)
```

### 2. Analysis (Repeatable, database-only)
```python
from services.pipeline.toid_analysis_pipeline import TOIDAnalysisPipeline

# Run analysis on existing data
pipeline = TOIDAnalysisPipeline()

# Check data availability first (optional)
availability = pipeline.check_data_availability(area_bbox)
if not availability["data_available"]:
    print(f"No data available: {availability['message']}")
    return

# Run analysis
result = pipeline.run_area_analysis(
    area_bbox=area_bbox,
    max_features=50
)
```

## Error Handling

### Missing Data Error
```python
{
    "success": False,
    "error": "No street data found in database for area [...]",
    "error_type": "missing_data",
    "recommendation": "Run StreetDataService.collect_street_data() to populate the database first"
}
```

### System Error
```python
{
    "success": False,
    "error": "Database connection failed: ...",
    "error_type": "system_error"
}
```

## Database Schema

The pipeline maintains proper relationships:

```sql
street_data (TOID + metadata)
    ↓ (street_data_id FK)
photos (images + location)
    ↓ (photo_id FK)
quality_results (image quality analysis)
    ↓ (photo_id FK)
road_analysis_results (road condition analysis)
```

## Utility Methods

- `check_data_availability(bbox)`: Check if data exists for an area
- `get_database_statistics()`: Get database statistics and coverage
- `get_pipeline_info()`: Get pipeline configuration and requirements

## Benefits

1. **Reliability**: No API failures during analysis
2. **Performance**: Fast database-only queries
3. **Scalability**: Process data offline without API rate limits
4. **Traceability**: Full lineage from TOID → metadata → images → analysis
5. **Testability**: Predictable behavior with known data
6. **Cost Control**: Minimize API calls by pre-collecting data

## Migration Guide

### Before (v2.0.0)
```python
# Old: Pipeline would fetch data on-demand
pipeline = TOIDAnalysisPipeline()
result = pipeline.run_area_analysis(bbox)  # Made API calls internally
```

### After (v3.0.0)
```python
# New: Two-phase approach
# Phase 1: Collect data (separate service)
street_service = StreetDataService()
street_service.collect_street_data(bbox=bbox)

# Phase 2: Analyze data (database-only)
pipeline = TOIDAnalysisPipeline()
result = pipeline.run_area_analysis(bbox)  # Database-only
```

## Best Practices

1. **Pre-populate data**: Always run `StreetDataService.collect_street_data()` first
2. **Check availability**: Use `check_data_availability()` before analysis
3. **Handle failures**: Always check `success` flag and `error_type`
4. **Monitor coverage**: Use `get_database_statistics()` to understand data coverage
5. **Batch processing**: Collect data for multiple areas, then analyze offline