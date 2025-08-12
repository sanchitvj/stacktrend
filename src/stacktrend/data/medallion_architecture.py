"""Medallion architecture schema definitions and data models for the technology adoption observatory."""

from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class LayerType(Enum):
    """Data layer types in medallion architecture."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class TechnologyCategory(Enum):
    """Technology categories for classification."""
    PROGRAMMING_LANGUAGE = "programming_language"
    FRAMEWORK = "framework"
    DATABASE = "database"
    DEVOPS_TOOL = "devops_tool"
    CLOUD_SERVICE = "cloud_service"
    FRONTEND_LIBRARY = "frontend_library"
    BACKEND_FRAMEWORK = "backend_framework"
    MOBILE_FRAMEWORK = "mobile_framework"
    DATA_TOOL = "data_tool"
    AI_ML_LIBRARY = "ai_ml_library"
    OTHER = "other"


class AdoptionLifecycle(Enum):
    """Technology adoption lifecycle stages."""
    EMERGING = "emerging"
    GROWING = "growing"
    MATURE = "mature"
    DECLINING = "declining"
    LEGACY = "legacy"


@dataclass
class BronzeRepositoryRecord:
    """Raw GitHub repository data structure (Bronze layer)."""
    repository_id: int
    name: str
    full_name: str
    owner_login: str
    owner_type: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime
    pushed_at: datetime
    language: Optional[str]
    stargazers_count: int
    watchers_count: int
    forks_count: int
    open_issues_count: int
    default_branch: str
    topics: List[str]
    license_name: Optional[str]
    archived: bool
    disabled: bool
    private: bool
    has_wiki: bool
    has_pages: bool
    has_projects: bool
    has_downloads: bool
    has_issues: bool
    size: int
    network_count: int
    subscribers_count: int
    homepage: Optional[str]
    api_response_raw: Dict[str, Any]  # Store full API response
    ingestion_timestamp: datetime
    partition_date: str  # YYYY-MM-DD format


@dataclass
class SilverRepositoryRecord:
    """Cleaned and standardized repository data (Silver layer)."""
    repository_id: int
    name: str
    full_name: str
    owner_login: str
    owner_type: str
    description_clean: Optional[str]
    created_at: datetime
    updated_at: datetime
    pushed_at: datetime
    primary_language: Optional[str]
    technology_category: TechnologyCategory
    stargazers_count: int
    watchers_count: int
    forks_count: int
    open_issues_count: int
    contributors_count: int  # Derived from API
    commit_frequency_30d: float  # Commits per day over last 30 days
    star_velocity_30d: float  # Stars gained per day over last 30 days
    community_health_score: float  # 0-100 based on various factors
    topics_standardized: List[str]
    license_category: Optional[str]  # Standardized license category
    is_active: bool  # Active if commits in last 90 days
    quality_score: float  # 0-100 based on documentation, tests, etc.
    data_quality_flags: List[str]  # Any data quality issues
    processed_timestamp: datetime
    partition_date: str


@dataclass
class GoldTechnologyMetrics:
    """Analytics-ready technology metrics (Gold layer)."""
    technology_name: str
    technology_category: TechnologyCategory
    adoption_lifecycle: AdoptionLifecycle
    measurement_date: datetime
    
    # Popularity Metrics
    total_repositories: int
    total_stars: int
    total_forks: int
    average_stars_per_repo: float
    
    # Growth Metrics
    star_velocity_7d: float
    star_velocity_30d: float
    star_velocity_90d: float
    repository_growth_rate: float
    
    # Community Health Metrics
    average_community_health_score: float
    active_repositories_percentage: float
    average_contributors_per_repo: float
    
    # Momentum Score (0-100)
    momentum_score: float
    momentum_trend: str  # "rising", "stable", "declining"
    
    # Risk Indicators
    single_maintainer_risk: float  # Percentage of repos with 1 maintainer
    license_diversity_score: float
    average_repository_age_days: int
    
    # Comparative Rankings
    popularity_rank: int
    growth_rank: int
    health_rank: int
    overall_rank: int
    
    partition_date: str


class DataPaths:
    """Standardized data paths for each layer."""
    
    @staticmethod
    def bronze_repository_path(date: str = None) -> str:
        """Path for bronze repository data."""
        if date:
            return f"bronze/github_repositories/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
        return "bronze/github_repositories/"
    
    @staticmethod
    def bronze_contributors_path(date: str = None) -> str:
        """Path for bronze contributor data."""
        if date:
            return f"bronze/github_contributors/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
        return "bronze/github_contributors/"
    
    @staticmethod
    def bronze_commits_path(date: str = None) -> str:
        """Path for bronze commit data."""
        if date:
            return f"bronze/github_commits/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
        return "bronze/github_commits/"
    
    @staticmethod
    def silver_repositories_path() -> str:
        """Path for silver repository data."""
        return "silver/repositories/"
    
    @staticmethod
    def silver_technologies_path() -> str:
        """Path for silver technology mapping."""
        return "silver/technologies/"
    
    @staticmethod
    def silver_metrics_path() -> str:
        """Path for silver calculated metrics."""
        return "silver/metrics/"
    
    @staticmethod
    def gold_technology_metrics_path(date: str = None) -> str:
        """Path for gold technology metrics."""
        if date:
            return f"gold/technology_metrics/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
        return "gold/technology_metrics/"
    
    @staticmethod
    def gold_trend_analysis_path() -> str:
        """Path for gold trend analysis data."""
        return "gold/trend_analysis/"
    
    @staticmethod
    def gold_predictions_path() -> str:
        """Path for gold prediction data."""
        return "gold/predictions/"


class SchemaValidation:
    """Schema validation utilities for data quality."""
    
    @staticmethod
    def validate_bronze_record(record: Dict[str, Any]) -> List[str]:
        """Validate bronze layer record and return list of issues."""
        issues = []
        
        required_fields = [
            "repository_id", "name", "full_name", "owner_login",
            "created_at", "stargazers_count", "ingestion_timestamp"
        ]
        
        for field in required_fields:
            if field not in record or record[field] is None:
                issues.append(f"Missing required field: {field}")
        
        # Validate data types and ranges
        if "stargazers_count" in record:
            if not isinstance(record["stargazers_count"], int) or record["stargazers_count"] < 0:
                issues.append("Invalid stargazers_count: must be non-negative integer")
        
        if "repository_id" in record:
            if not isinstance(record["repository_id"], int) or record["repository_id"] <= 0:
                issues.append("Invalid repository_id: must be positive integer")
        
        return issues
    
    @staticmethod
    def validate_silver_record(record: Dict[str, Any]) -> List[str]:
        """Validate silver layer record and return list of issues."""
        issues = []
        
        # Check momentum scores are within valid ranges
        score_fields = ["community_health_score", "quality_score"]
        for field in score_fields:
            if field in record:
                value = record[field]
                if not isinstance(value, (int, float)) or not 0 <= value <= 100:
                    issues.append(f"Invalid {field}: must be between 0 and 100")
        
        # Validate velocity calculations
        velocity_fields = ["commit_frequency_30d", "star_velocity_30d"]
        for field in velocity_fields:
            if field in record:
                value = record[field]
                if not isinstance(value, (int, float)) or value < 0:
                    issues.append(f"Invalid {field}: must be non-negative number")
        
        return issues
    
    @staticmethod
    def validate_gold_record(record: Dict[str, Any]) -> List[str]:
        """Validate gold layer record and return list of issues."""
        issues = []
        
        # Validate momentum score
        if "momentum_score" in record:
            value = record["momentum_score"]
            if not isinstance(value, (int, float)) or not 0 <= value <= 100:
                issues.append("Invalid momentum_score: must be between 0 and 100")
        
        # Validate ranking fields are positive
        ranking_fields = ["popularity_rank", "growth_rank", "health_rank", "overall_rank"]
        for field in ranking_fields:
            if field in record:
                value = record[field]
                if not isinstance(value, int) or value <= 0:
                    issues.append(f"Invalid {field}: must be positive integer")
        
        return issues


# Export key classes and functions
__all__ = [
    "LayerType",
    "TechnologyCategory", 
    "AdoptionLifecycle",
    "BronzeRepositoryRecord",
    "SilverRepositoryRecord", 
    "GoldTechnologyMetrics",
    "DataPaths",
    "SchemaValidation"
]