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
    """Create database engine based on configuration."""
    if settings.debug:
        # Use SQLite for development
        database_url = "sqlite:///./korean_stocks.db"
        engine = create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            echo=True
        )
        logger.info("Using SQLite database for development")
    else:
        # Use PostgreSQL for production
        if not settings.database_url:
            raise ValueError("DATABASE_URL must be set for production")
        
        engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=settings.debug
        )
        logger.info("Using PostgreSQL database for production")
    
    return engine

# Create engine instance
engine = create_database_engine()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create base class for models
Base = declarative_base()

def get_db() -> Generator[Session, None, None]:
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Initialize database tables."""
    try:
        # Import all models to ensure they are registered
        from src.data_collection.korean_stocks import KoreanStock
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def close_db():
    """Close database connections."""
    try:
        engine.dispose()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error closing database: {e}")

# Korean stocks specific database functions
def get_korean_stocks_db_url() -> str:
    """Get database URL for Korean stocks data."""
    if settings.debug:
        return "sqlite:///./korean_stocks.db"
    else:
        # Use a separate database for Korean stocks data
        return settings.database_url.replace("/invest", "/korean_stocks") if settings.database_url else None

def create_korean_stocks_engine():
    """Create engine specifically for Korean stocks data."""
    database_url = get_korean_stocks_db_url()
    if not database_url:
        raise ValueError("Database URL not configured")
    
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=300,
        echo=settings.debug
    ) 