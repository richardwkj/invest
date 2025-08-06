"""
Main settings configuration for the Invest Analytics application.
This file contains all application settings and configuration parameters.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # Application
    app_name: str = "Invest Analytics"
    version: str = "0.1.0"
    debug: bool = False
    environment: str = "development"
    
    # Database
    database_url: str = "sqlite:///./invest_analytics.db"
    database_url_test: Optional[str] = None
    
    # API Keys
    dart_api_key: Optional[str] = None
    news_api_key: Optional[str] = None
    alpha_vantage_api_key: Optional[str] = None
    yahoo_finance_api_key: Optional[str] = None
    
    # AI/ML Configuration
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    huggingface_api_key: Optional[str] = None
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    
    # Data Collection
    data_collection_interval: int = 3600  # seconds
    max_retries: int = 3
    request_timeout: int = 30
    
    # File Storage
    data_storage_path: str = "./data"
    model_storage_path: str = "./data/models"
    backup_storage_path: str = "./data/backups"
    
    # Logging
    log_level: str = "INFO"
    log_file: str = "./logs/app.log"
    
    # Security
    secret_key: str = "your-secret-key-change-in-production"
    encryption_key: Optional[str] = None
    jwt_secret_key: Optional[str] = None
    
    # Monitoring
    prometheus_port: int = 9090
    grafana_port: int = 3000
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the application settings."""
    return settings 