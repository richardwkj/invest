"""
Database configuration and connection management.
This file handles database setup, connection pooling, and session management.
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from typing import Generator
import logging

from .settings import settings

# Configure logging
logger = logging.getLogger(__name__)

# Create SQLAlchemy engine
def create_database_engine():
    """Create and configure the database engine."""
    try:
        if settings.environment == "test":
            # Use in-memory SQLite for testing
            engine = create_engine(
                "sqlite:///:memory:",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                echo=settings.debug
            )
        else:
            # Use configured database URL
            engine = create_engine(
                settings.database_url,
                echo=settings.debug,
                pool_pre_ping=True,
                pool_recycle=300
            )
        
        logger.info(f"Database engine created successfully for environment: {settings.environment}")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

# Create engine instance
engine = create_database_engine()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()

def get_db() -> Generator[Session, None, None]:
    """
    Dependency function to get database session.
    Yields a database session and ensures it's closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def init_db():
    """Initialize the database by creating all tables."""
    try:
        # Import all models here to ensure they're registered
        from src.database import models
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def close_db():
    """Close the database engine."""
    try:
        engine.dispose()
        logger.info("Database engine closed successfully")
    except Exception as e:
        logger.error(f"Failed to close database engine: {e}")
        raise 