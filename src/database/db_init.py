"""
Database initialization script for Road Quality Analysis system.

This module provides functionality to:
- Drop and recreate the entire database schema
- Initialize fresh tables, types, and indexes
- Handle schema migrations during development

Usage:
    python -m src.database.db_init --reset  # Drop all and recreate
    python -m src.database.db_init --init   # Create if not exists
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DatabaseInitializer:
    """Handles database initialization and schema management."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
    ):
        """Initialize with database connection parameters."""

        # Use provided params or fall back to environment variables
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = port or int(os.getenv("DB_PORT", 5432))
        self.database = database or os.getenv("DB_NAME", "road_quality")
        self.user = user or os.getenv("DB_USER", "postgres")
        self.password = password or os.getenv("DB_PASSWORD")

        if not self.password:
            raise ValueError(
                "Database password must be provided via DB_PASSWORD env var or constructor"
            )

        # Path to schema file
        self.schema_file = Path(__file__).parent.parent.parent / "database_schema.sql"

        if not self.schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_file}")

    def get_connection(self, database: str = None) -> psycopg2.extensions.connection:
        """Get database connection."""
        db_name = database or self.database

        conn = psycopg2.connect(
            host=self.host, port=self.port, database=db_name, user=self.user, password=self.password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def database_exists(self) -> bool:
        """Check if the target database exists."""
        try:
            # Connect to postgres database to check if target exists
            conn = self.get_connection("postgres")
            cursor = conn.cursor()

            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.database,))
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()

            return exists

        except psycopg2.Error as e:
            logger.error(f"Error checking database existence: {e}")
            return False

    def create_database(self) -> None:
        """Create the target database if it doesn't exist."""
        if self.database_exists():
            logger.info(f"Database '{self.database}' already exists")
            return

        try:
            # Connect to postgres database to create target
            conn = self.get_connection("postgres")
            cursor = conn.cursor()

            # Create database (cannot use parameterized query for database name)
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.database)))

            logger.info(f"Created database '{self.database}'")

            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            logger.error(f"Error creating database: {e}")
            raise

    def drop_database(self) -> None:
        """Drop the target database if it exists."""
        if not self.database_exists():
            logger.info(f"Database '{self.database}' does not exist, nothing to drop")
            return

        try:
            # Connect to postgres database to drop target
            conn = self.get_connection("postgres")
            cursor = conn.cursor()

            # Terminate active connections to target database
            cursor.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
            """,
                (self.database,),
            )

            # Drop database
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(self.database))
            )

            logger.info(f"Dropped database '{self.database}'")

            cursor.close()
            conn.close()

        except psycopg2.Error as e:
            logger.error(f"Error dropping database: {e}")
            raise

    def drop_all_schema_objects(self) -> None:
        """Drop all tables, types, and other schema objects in the target database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            logger.info("Dropping all schema objects...")

            # Drop views first (they depend on tables)
            cursor.execute("""
                DROP VIEW IF EXISTS photo_analysis_summary CASCADE;
                DROP VIEW IF EXISTS street_quality_summary CASCADE;
            """)

            # Drop tables in reverse dependency order
            tables = [
                "group_analysis_photos",
                "road_analysis_groups",
                "road_analysis_results",
                "quality_results",
                "photos",
                "street_points",
                "streets",
            ]

            for table in tables:
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table))
                )
                logger.info(f"Dropped table: {table}")

            # Drop custom types
            types = [
                "crack_severity",
                "image_source",
                "accessibility_type",
                "road_quality_rating",
                "road_surface_type",
            ]

            for type_name in types:
                cursor.execute(
                    sql.SQL("DROP TYPE IF EXISTS {} CASCADE").format(sql.Identifier(type_name))
                )
                logger.info(f"Dropped type: {type_name}")

            cursor.close()
            conn.close()

            logger.info("Successfully dropped all schema objects")

        except psycopg2.Error as e:
            logger.error(f"Error dropping schema objects: {e}")
            raise

    def run_schema_file(self) -> None:
        """Execute the schema SQL file to create all objects."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            logger.info(f"Executing schema file: {self.schema_file}")

            # Read and execute schema file
            with open(self.schema_file) as f:
                schema_sql = f.read()

            cursor.execute(schema_sql)

            cursor.close()
            conn.close()

            logger.info("Successfully created all schema objects")

        except psycopg2.Error as e:
            logger.error(f"Error executing schema file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading schema file: {e}")
            raise

    def validate_schema(self) -> bool:
        """Validate that all expected tables and types exist."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Check tables
            expected_tables = [
                "streets",
                "street_points",
                "photos",
                "quality_results",
                "road_analysis_results",
            ]

            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]

            missing_tables = set(expected_tables) - set(existing_tables)
            if missing_tables:
                logger.error(f"Missing tables: {missing_tables}")
                return False

            # Check custom types
            expected_types = [
                "road_surface_type",
                "road_quality_rating",
                "accessibility_type",
                "image_source",
                "crack_severity",
            ]

            cursor.execute("""
                SELECT typname
                FROM pg_type
                WHERE typtype = 'e'
            """)
            existing_types = [row[0] for row in cursor.fetchall()]

            missing_types = set(expected_types) - set(existing_types)
            if missing_types:
                logger.error(f"Missing types: {missing_types}")
                return False

            # Check PostGIS extension
            cursor.execute("""
                SELECT 1 FROM pg_extension WHERE extname = 'postgis'
            """)
            if not cursor.fetchone():
                logger.error("PostGIS extension not installed")
                return False

            cursor.close()
            conn.close()

            logger.info("Schema validation passed")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error validating schema: {e}")
            return False

    def init_fresh_database(self) -> None:
        """Complete database initialization from scratch."""
        logger.info("Starting fresh database initialization...")

        # Create database if needed
        self.create_database()

        # Run schema file
        self.run_schema_file()

        # Validate
        if self.validate_schema():
            logger.info("✅ Database initialization completed successfully")
        else:
            logger.error("❌ Database initialization failed validation")
            raise Exception("Schema validation failed")

    def reset_database(self) -> None:
        """Drop everything and recreate from scratch."""
        logger.info("Starting database reset...")

        # Option 1: Drop entire database and recreate
        # self.drop_database()
        # self.init_fresh_database()

        # Option 2: Drop only schema objects (faster, preserves database)
        self.create_database()  # Ensure database exists
        self.drop_all_schema_objects()
        self.run_schema_file()

        # Validate
        if self.validate_schema():
            logger.info("✅ Database reset completed successfully")
        else:
            logger.error("❌ Database reset failed validation")
            raise Exception("Schema validation failed")


def main():
    """Command line interface for database initialization."""
    parser = argparse.ArgumentParser(description="Road Quality Database Initializer")
    parser.add_argument(
        "--reset", action="store_true", help="Drop all schema objects and recreate from scratch"
    )
    parser.add_argument(
        "--init", action="store_true", help="Initialize database (create if not exists)"
    )
    parser.add_argument("--validate", action="store_true", help="Validate existing schema")
    parser.add_argument("--drop-db", action="store_true", help="Drop entire database (DESTRUCTIVE)")

    args = parser.parse_args()

    if not any([args.reset, args.init, args.validate, args.drop_db]):
        parser.print_help()
        sys.exit(1)

    try:
        db_init = DatabaseInitializer()

        if args.drop_db:
            confirm = input(
                f"Are you sure you want to drop database '{db_init.database}'? (yes/no): "
            )
            if confirm.lower() == "yes":
                db_init.drop_database()
            else:
                logger.info("Database drop cancelled")

        elif args.reset:
            confirm = input(
                f"Reset will drop all data in '{db_init.database}'. Continue? (yes/no): "
            )
            if confirm.lower() == "yes":
                db_init.reset_database()
            else:
                logger.info("Database reset cancelled")

        elif args.init:
            db_init.init_fresh_database()

        elif args.validate:
            if db_init.validate_schema():
                logger.info("✅ Schema validation passed")
            else:
                logger.error("❌ Schema validation failed")
                sys.exit(1)

    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
