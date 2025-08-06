"""
API keys management and configuration.
This file handles API key validation and access for various data sources.
"""

import os
from typing import Dict, Optional
from dataclasses import dataclass
import logging

from .settings import settings

logger = logging.getLogger(__name__)

@dataclass
class APIKeyConfig:
    """Configuration for API keys with validation."""
    key: str
    required: bool = True
    description: str = ""
    
    def is_valid(self) -> bool:
        """Check if the API key is valid (not empty and properly formatted)."""
        if not self.key:
            return False
        return len(self.key.strip()) > 0

class APIKeyManager:
    """Manages API keys for various data sources."""
    
    def __init__(self):
        self.keys: Dict[str, APIKeyConfig] = {}
        self._load_api_keys()
    
    def _load_api_keys(self):
        """Load API keys from settings."""
        # Korean market data APIs
        self.keys["dart"] = APIKeyConfig(
            key=settings.dart_api_key or "",
            required=True,
            description="DART API key for Korean financial data"
        )
        
        # News and sentiment APIs
        self.keys["news"] = APIKeyConfig(
            key=settings.news_api_key or "",
            required=False,
            description="News API key for sentiment analysis"
        )
        
        # US market data APIs (Phase 3)
        self.keys["alpha_vantage"] = APIKeyConfig(
            key=settings.alpha_vantage_api_key or "",
            required=False,
            description="Alpha Vantage API key for US market data"
        )
        
        self.keys["yahoo_finance"] = APIKeyConfig(
            key=settings.yahoo_finance_api_key or "",
            required=False,
            description="Yahoo Finance API key for US market data"
        )
        
        # AI/ML APIs
        self.keys["openai"] = APIKeyConfig(
            key=settings.openai_api_key or "",
            required=False,
            description="OpenAI API key for AI analysis"
        )
        
        self.keys["anthropic"] = APIKeyConfig(
            key=settings.anthropic_api_key or "",
            required=False,
            description="Anthropic API key for AI analysis"
        )
        
        self.keys["huggingface"] = APIKeyConfig(
            key=settings.huggingface_api_key or "",
            required=False,
            description="Hugging Face API key for ML models"
        )
    
    def get_key(self, service: str) -> Optional[str]:
        """Get API key for a specific service."""
        if service not in self.keys:
            logger.warning(f"Unknown API service: {service}")
            return None
        
        config = self.keys[service]
        if not config.is_valid():
            if config.required:
                logger.error(f"Required API key missing for {service}: {config.description}")
                return None
            else:
                logger.warning(f"Optional API key missing for {service}: {config.description}")
                return None
        
        return config.key
    
    def validate_keys(self) -> Dict[str, bool]:
        """Validate all API keys and return status."""
        validation_results = {}
        
        for service, config in self.keys.items():
            is_valid = config.is_valid()
            validation_results[service] = is_valid
            
            if config.required and not is_valid:
                logger.error(f"Required API key missing: {service}")
            elif not is_valid:
                logger.warning(f"Optional API key missing: {service}")
            else:
                logger.info(f"API key validated: {service}")
        
        return validation_results
    
    def get_missing_required_keys(self) -> list:
        """Get list of missing required API keys."""
        missing_keys = []
        
        for service, config in self.keys.items():
            if config.required and not config.is_valid():
                missing_keys.append(service)
        
        return missing_keys
    
    def is_ready_for_phase(self, phase: int) -> bool:
        """Check if API keys are ready for a specific development phase."""
        if phase == 1:  # Korean markets
            return self.keys["dart"].is_valid()
        elif phase == 2:  # AI agents
            return (
                self.keys["dart"].is_valid() and
                (self.keys["openai"].is_valid() or 
                 self.keys["anthropic"].is_valid() or 
                 self.keys["huggingface"].is_valid())
            )
        elif phase == 3:  # US markets
            return (
                self.keys["dart"].is_valid() and
                (self.keys["alpha_vantage"].is_valid() or 
                 self.keys["yahoo_finance"].is_valid())
            )
        else:
            return False

# Global API key manager instance
api_key_manager = APIKeyManager()

def get_api_key(service: str) -> Optional[str]:
    """Convenience function to get API key for a service."""
    return api_key_manager.get_key(service)

def validate_api_keys() -> Dict[str, bool]:
    """Convenience function to validate all API keys."""
    return api_key_manager.validate_keys() 