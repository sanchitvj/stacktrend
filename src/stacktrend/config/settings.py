"""Configuration settings for the StackTrend application."""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""
    
    # Azure Storage Configuration
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
    # GitHub API Configuration
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_API_BASE_URL = "https://api.github.com"
    
    # Azure Data Factory Configuration
    ADF_SUBSCRIPTION_ID = os.getenv("ADF_SUBSCRIPTION_ID")
    ADF_RESOURCE_GROUP = os.getenv("ADF_RESOURCE_GROUP", "tech-adoption-rg")
    ADF_DATA_FACTORY_NAME = os.getenv("ADF_DATA_FACTORY_NAME", "tech-adoption-adf")
    
    # Data Storage Containers
    BRONZE_CONTAINER = "bronze"
    SILVER_CONTAINER = "silver" 
    GOLD_CONTAINER = "gold"
    
    # GitHub API Rate Limiting
    GITHUB_RATE_LIMIT_PER_HOUR = 5000
    REQUESTS_PER_MINUTE = 80  # Conservative rate limiting
    
    @classmethod
    def validate(cls):
        """Validate that required settings are present."""
        required_settings = [
            "AZURE_STORAGE_ACCOUNT_NAME",
            "AZURE_STORAGE_ACCOUNT_KEY", 
            "GITHUB_TOKEN"
        ]
        
        missing = []
        for setting in required_settings:
            if not getattr(cls, setting):
                missing.append(setting)
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


# Create settings instance
settings = Settings() 