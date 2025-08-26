"""
Kiwoom API Configuration
Contains API credentials and configuration settings for Kiwoom integration.
"""

import os
from typing import Optional
from dataclasses import dataclass

@dataclass
class KiwoomConfig:
    """Kiwoom API configuration settings"""
    
    # API Credentials (should be loaded from environment variables)
    app_key: str = ""
    secret_key: str = ""
    
    # API URLs
    mock_api_url: str = "https://mockapi.kiwoom.com"
    production_api_url: str = "https://api.kiwoom.com"
    
    # Environment
    is_production: bool = False
    
    # Rate Limiting
    rate_limit_delay: float = 1.0  # seconds between API calls
    max_retries: int = 3
    retry_delay: float = 5.0
    
    @property
    def api_url(self) -> str:
        """Get the appropriate API URL based on environment"""
        return self.production_api_url if self.is_production else self.mock_api_url
    
    @property
    def auth_data(self) -> dict:
        """Get authentication data for OAuth token request"""
        return {
            'grant_type': 'client_credentials',
            'appkey': self.app_key,
            'secretkey': self.secret_key
        }
    
    @classmethod
    def from_env(cls) -> 'KiwoomConfig':
        """Create configuration from environment variables"""
        app_key = os.getenv('KIWOOM_APP_KEY')
        secret_key = os.getenv('KIWOOM_SECRET_KEY')
        
        if not app_key:
            raise ValueError("KIWOOM_APP_KEY environment variable is required")
        if not secret_key:
            raise ValueError("KIWOOM_SECRET_KEY environment variable is required")
        
        return cls(
            app_key=app_key,
            secret_key=secret_key,
            mock_api_url=os.getenv('KIWOOM_MOCK_API_URL', cls.mock_api_url),
            production_api_url=os.getenv('KIWOOM_PRODUCTION_API_URL', cls.production_api_url),
            is_production=os.getenv('KIWOOM_PRODUCTION', 'false').lower() == 'true',
            rate_limit_delay=float(os.getenv('KIWOOM_RATE_LIMIT_DELAY', str(cls.rate_limit_delay))),
            max_retries=int(os.getenv('KIWOOM_MAX_RETRIES', str(cls.max_retries))),
            retry_delay=float(os.getenv('KIWOOM_RETRY_DELAY', str(cls.retry_delay)))
        )

# Default configuration instance
kiwoom_config = KiwoomConfig()

# Environment-specific configurations
def get_kiwoom_config(environment: str = 'development') -> KiwoomConfig:
    """
    Get Kiwoom configuration for specific environment
    
    Args:
        environment: 'development', 'testing', or 'production'
    
    Returns:
        KiwoomConfig instance
    """
    config = KiwoomConfig.from_env()
    
    if environment == 'production':
        config.is_production = True
    elif environment == 'testing':
        config.rate_limit_delay = 0.1  # Faster for testing
        config.max_retries = 1
    
    return config

# Usage examples:
# config = get_kiwoom_config('development')
# print(f"API URL: {config.api_url}")
# print(f"Auth Data: {config.auth_data}")
