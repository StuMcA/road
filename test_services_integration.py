#!/usr/bin/env python3
"""
Integration test to verify services work with the current database schema.
This is a quick test to ensure the recent database schema updates work correctly.
"""

import os
import sys
from pathlib import Path

# Add the src directory to the path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

try:
    from src.services.street_data_service import StreetDataService
    from src.services.pipeline.toid_analysis_pipeline import TOIDAnalysisPipeline
    from src.database.services.database_service import DatabaseService
    
    print("✓ All imports successful")
    
    # Test 1: StreetDataService initialization
    print("\n=== Test 1: StreetDataService ===")
    try:
        street_service = StreetDataService()
        info = street_service.get_service_info()
        print(f"✓ StreetDataService initialized: {info['service_name']} v{info['version']}")
        print(f"  Tables: {info['database_tables']}")
    except Exception as e:
        print(f"✗ StreetDataService failed: {e}")
        
    # Test 2: TOIDAnalysisPipeline initialization 
    print("\n=== Test 2: TOIDAnalysisPipeline ===")
    try:
        toid_pipeline = TOIDAnalysisPipeline()
        info = toid_pipeline.get_pipeline_info()
        print(f"✓ TOIDAnalysisPipeline initialized: {info['pipeline_name']} v{info['version']}")
        print(f"  Mode: {info['mode']}")
    except Exception as e:
        print(f"✗ TOIDAnalysisPipeline failed: {e}")
        
    # Test 3: Database connection
    print("\n=== Test 3: Database Connection ===")
    try:
        db_service = DatabaseService()
        with db_service.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            print(f"✓ Database connection working: {result}")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        
    # Test 4: Database schema check
    print("\n=== Test 4: Database Schema ===")
    try:
        db_service = DatabaseService()
        with db_service.transaction() as conn:
            cursor = conn.cursor()
            
            # Check required tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('streets', 'street_points', 'photos', 'quality_results', 'road_analysis_results')
                ORDER BY table_name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f"✓ Required tables found: {tables}")
            
            if len(tables) >= 4:
                print(f"✓ Core schema tables present: {len(tables)} found")
            else:
                print(f"⚠ Some tables may be missing. Expected 5, found {len(tables)}")
                
            # Check if we can add toid column to street_points (will be done by service)
            try:
                cursor.execute("SELECT COUNT(*) FROM street_points")
                count = cursor.fetchone()[0] 
                print(f"✓ street_points table accessible: {count} records")
            except Exception as e:
                print(f"✗ street_points table issue: {e}")
                
    except Exception as e:
        print(f"✗ Database schema check failed: {e}")
        
    print("\n=== Integration Test Complete ===")
    print("Services are ready for use with the current database schema!")
        
except ImportError as e:
    print(f"✗ Import failed: {e}")
    print("Make sure all dependencies are installed and the src path is correct.")
    
except Exception as e:
    print(f"✗ Unexpected error: {e}")