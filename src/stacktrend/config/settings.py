"""Configuration settings for the StackTrend application."""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""
    
    # Azure Storage Configuration (Legacy - keeping for backward compatibility)
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
    # GitHub API Configuration
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    GITHUB_API_BASE_URL = "https://api.github.com"
    
    # Microsoft Fabric Configuration
    FABRIC_WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
    FABRIC_TENANT_ID = os.getenv("FABRIC_TENANT_ID")
    FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
    FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
    FABRIC_WORKSPACE_NAME = os.getenv("FABRIC_WORKSPACE_NAME", "technology-adoption-observatory")
    
    # Lakehouse Configuration
    BRONZE_LAKEHOUSE_NAME = os.getenv("BRONZE_LAKEHOUSE_NAME", "stacktrend_bronze_lh")
    SILVER_LAKEHOUSE_NAME = os.getenv("SILVER_LAKEHOUSE_NAME", "stacktrend_silver_lh")
    GOLD_LAKEHOUSE_NAME = os.getenv("GOLD_LAKEHOUSE_NAME", "stacktrend_gold_lh")
    
    # Data Factory Pipeline Names
    GITHUB_INGESTION_PIPELINE = "github_data_ingestion"
    BRONZE_TO_SILVER_PIPELINE = "bronze_to_silver_processing"
    SILVER_TO_GOLD_PIPELINE = "silver_to_gold_analytics"
    
    # Data Storage Containers (Legacy)
    BRONZE_CONTAINER = "bronze"
    SILVER_CONTAINER = "silver" 
    GOLD_CONTAINER = "gold"
    
    # GitHub API Rate Limiting
    GITHUB_RATE_LIMIT_PER_HOUR = 5000
    REQUESTS_PER_MINUTE = 80  # Conservative rate limiting
    
    # Data Processing Configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
    MAX_REPOSITORIES = int(os.getenv("MAX_REPOSITORIES", "1000"))
    DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "90"))
    
    # Cost Management
    MAX_MONTHLY_BUDGET = float(os.getenv("MAX_MONTHLY_BUDGET", "50.0"))
    ENABLE_COST_ALERTS = os.getenv("ENABLE_COST_ALERTS", "true").lower() == "true"
    
    # Power BI Configuration
    POWERBI_WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")  # Can be same as Fabric workspace
    POWERBI_DATASET_NAME = "technology_trends_dataset"
    
    # Azure OpenAI Configuration for LLM Classification
    AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
    AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
    AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")
    AZURE_OPENAI_MODEL = os.getenv("AZURE_OPENAI_MODEL", "gpt-4.1-mini")
    
    @classmethod
    def validate(cls):
        """Validate that required settings are present."""
        required_settings = [
            "GITHUB_TOKEN",
            "FABRIC_WORKSPACE_ID",
            "FABRIC_TENANT_ID",
            "FABRIC_CLIENT_ID"
        ]
        
        missing = []
        for setting in required_settings:
            if not getattr(cls, setting):
                missing.append(setting)
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    @classmethod
    def get_fabric_auth_config(cls):
        """Returns Fabric authentication configuration as a dictionary."""
        return {
            "tenant_id": cls.FABRIC_TENANT_ID,
            "client_id": cls.FABRIC_CLIENT_ID,
            "client_secret": cls.FABRIC_CLIENT_SECRET,
            "workspace_id": cls.FABRIC_WORKSPACE_ID
        }
    
    @classmethod
    def get_lakehouse_paths(cls):
        """Returns lakehouse storage paths for each layer."""
        return {
            "bronze": f"abfss://{cls.BRONZE_LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/",
            "silver": f"abfss://{cls.SILVER_LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/",
            "gold": f"abfss://{cls.GOLD_LAKEHOUSE_NAME}@onelake.dfs.fabric.microsoft.com/"
        }


# Create settings instance
settings = Settings() 