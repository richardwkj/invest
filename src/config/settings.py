"""
Main application settings and configuration
Integrates all configuration sources including Kiwoom API
"""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field

from .kiwoom_config import KiwoomConfig, get_kiwoom_config

class Settings(BaseSettings):
    """Main application settings"""
    
    # Application
    app_name: str = "Investment Analytics Platform"
    version: str = "1.0.0"
    debug: bool = Field(default=True, env="DEBUG")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file_path: str = Field(default="logs/app.log", env="LOG_FILE_PATH")
    log_max_size: str = Field(default="10MB", env="LOG_MAX_SIZE")
    log_backup_count: int = Field(default=5, env="LOG_BACKUP_COUNT")
    
    # Database
    database_url: str = Field(
        default="sqlite:///data/raw/korean_stocks/kr_stock_codes.db",
        env="DATABASE_URL"
    )
    database_url_dev: str = Field(
        default="sqlite:///data/raw/korean_stocks/kr_stock_codes.db",
        env="DATABASE_URL_DEV"
    )
    database_url_prod: str = Field(
        default="postgresql://user:pass@localhost:5432/invest",
        env="DATABASE_URL_PROD"
    )
    
    # Security
    secret_key: str = Field(
        default="your_secret_key_here_change_in_production",
        env="SECRET_KEY"
    )
    algorithm: str = Field(default="HS256", env="ALGORITHM")
    access_token_expire_minutes: int = Field(
        default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES"
    )
    
    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379/0", env="REDIS_URL"
    )
    
    # Celery
    celery_broker_url: str = Field(
        default="redis://localhost:6379/0", env="CELERY_BROKER_URL"
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/0", env="CELERY_RESULT_BACKEND"
    )
    
    # API Rate Limiting
    api_rate_limit: int = Field(default=100, env="API_RATE_LIMIT")
    api_rate_limit_window: int = Field(default=3600, env="API_RATE_LIMIT_WINDOW")
    
    # Data Collection
    data_collection_delay: float = Field(default=1.0, env="DATA_COLLECTION_DELAY")
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: float = Field(default=5.0, env="RETRY_DELAY")
    
    # External APIs
    dart_api_key: Optional[str] = Field(default=None, env="DART_API_KEY")
    yahoo_finance_api_key: Optional[str] = Field(
        default=None, env="YAHOO_FINANCE_API_KEY"
    )
    
    # Paths
    base_dir: Path = Path(__file__).parent.parent.parent
    data_dir: Path = base_dir / "data"
    logs_dir: Path = base_dir / "logs"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    @property
    def kiwoom(self) -> KiwoomConfig:
        """Get Kiwoom configuration"""
        return get_kiwoom_config(self.environment)
    
    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.environment.lower() == "development"
    
    @property
    def is_testing(self) -> bool:
        """Check if running in testing"""
        return self.environment.lower() == "testing"
    
    def get_database_url(self) -> str:
        """Get appropriate database URL based on environment"""
        if self.is_production:
            return self.database_url_prod
        elif self.is_testing:
            return "sqlite:///:memory:"
        else:
            return self.database_url_dev
    
    def ensure_directories(self):
        """Ensure required directories exist"""
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        (self.data_dir / "raw" / "korean_stocks").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "processed").mkdir(exist_ok=True)
        (self.data_dir / "analysis").mkdir(exist_ok=True)

# Global settings instance
settings = Settings()

# Ensure directories exist on import
settings.ensure_directories()

# Convenience functions
def get_settings() -> Settings:
    """Get application settings"""
    return settings

def get_kiwoom_settings() -> KiwoomConfig:
    """Get Kiwoom-specific settings"""
    return settings.kiwoom

# Usage examples:
# from src.config.settings import settings, get_kiwoom_settings
# 
# # Access general settings
# print(f"Database URL: {settings.get_database_url()}")
# print(f"Environment: {settings.environment}")
# 
# # Access Kiwoom settings
# kiwoom_config = get_kiwoom_settings()
# print(f"Kiwoom API URL: {kiwoom_config.api_url}")
# print(f"Kiwoom App Key: {kiwoom_config.app_key}")
