"""
GitHub Data Collection Utility for Testing

Collects sample GitHub repository data and saves it in the format
expected by our Bronze layer for testing the transformation pipeline.
"""

import requests
import json
import os
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
import logging
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class GitHubDataCollector:
    """Collect GitHub repository data for testing."""
    
    def __init__(self, settings: Settings = None):
        """Initialize collector with GitHub API settings."""
        self.settings = settings or Settings()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {self.settings.GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "stacktrend-observatory"
        })
    
    def get_top_repositories(self, count: int = 50) -> List[Dict[str, Any]]:
        """Get top repositories by stars for testing."""
        logger.info(f"Fetching top {count} repositories...")
        
        repositories = []
        per_page = min(count, 100)  # GitHub API limit
        pages_needed = (count + per_page - 1) // per_page
        
        for page in range(1, pages_needed + 1):
            params = {
                "q": "stars:>1000",
                "sort": "stars",
                "order": "desc",
                "per_page": per_page,
                "page": page
            }
            
            try:
                response = self.session.get(
                    "https://api.github.com/search/repositories",
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                repositories.extend(data.get("items", []))
                
                # Respect rate limits
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                if remaining < 10:
                    logger.warning("Approaching rate limit, stopping collection")
                    break
                    
                logger.info(f"Collected page {page}, total repos: {len(repositories)}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        return repositories[:count]
    
    def transform_to_bronze_format(self, repositories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform GitHub API response to Bronze layer format."""
        bronze_records = []
        current_time = datetime.now()
        partition_date = current_time.strftime("%Y-%m-%d")
        
        for repo in repositories:
            try:
                # Extract and standardize fields
                bronze_record = {
                    "repository_id": repo["id"],
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "owner_login": repo["owner"]["login"],
                    "owner_type": repo["owner"]["type"],
                    "description": repo.get("description"),
                    "created_at": repo["created_at"],
                    "updated_at": repo["updated_at"],
                    "pushed_at": repo["pushed_at"],
                    "language": repo.get("language"),
                    "stargazers_count": repo["stargazers_count"],
                    "watchers_count": repo["watchers_count"],
                    "forks_count": repo["forks_count"],
                    "open_issues_count": repo["open_issues_count"],
                    "default_branch": repo["default_branch"],
                    "topics": repo.get("topics", []),
                    "license_name": repo["license"]["name"] if repo.get("license") else None,
                    "archived": repo["archived"],
                    "disabled": repo["disabled"],
                    "private": repo["private"],
                    "has_wiki": repo["has_wiki"],
                    "has_pages": repo["has_pages"],
                    "has_projects": repo["has_projects"],
                    "has_downloads": repo["has_downloads"],
                    "has_issues": repo["has_issues"],
                    "size": repo.get("size", 0),
                    "network_count": repo.get("forks_count", 0),  # Use forks as network count
                    "subscribers_count": repo.get("subscribers_count", 0),
                    "homepage": repo.get("homepage"),
                    "api_response_raw": repo,  # Store full response
                    "ingestion_timestamp": current_time.isoformat(),
                    "partition_date": partition_date
                }
                
                bronze_records.append(bronze_record)
                
            except Exception as e:
                logger.error(f"Error processing repository {repo.get('full_name', 'unknown')}: {e}")
                continue
        
        return bronze_records
    
    def save_bronze_data(self, records: List[Dict[str, Any]], output_dir: str = "test_data") -> str:
        """Save bronze records to local files for testing."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Create partition structure
        partition_date = datetime.now().strftime("%Y-%m-%d")
        year, month, day = partition_date.split("-")
        
        partition_dir = os.path.join(
            output_dir, 
            "bronze", 
            "github_repositories",
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )
        os.makedirs(partition_dir, exist_ok=True)
        
        # Save as JSON (simpler for testing)
        json_file = os.path.join(partition_dir, f"repo_metadata_{partition_date}.json")
        with open(json_file, 'w') as f:
            json.dump(records, f, indent=2, default=str)
        
        logger.info(f"Saved {len(records)} records to {json_file}")
        
        # Also save as CSV for easy inspection
        csv_file = os.path.join(partition_dir, f"repo_metadata_{partition_date}.csv")
        df = pd.DataFrame(records)
        
        # Flatten complex fields for CSV
        columns_to_drop = [col for col in ['api_response_raw', 'topics'] if col in df.columns]
        df_simple = df.drop(columns_to_drop, axis=1)
        if 'topics' in df.columns:
            df_simple['topics_count'] = df['topics'].apply(len)
        df_simple.to_csv(csv_file, index=False)
        
        logger.info(f"Also saved simplified CSV to {csv_file}")
        
        return json_file
    
    def collect_sample_data(self, count: int = 50, output_dir: str = "test_data") -> str:
        """Complete workflow: collect, transform, and save sample data."""
        logger.info(f"Starting sample data collection for {count} repositories...")
        
        # Collect repositories
        repositories = self.get_top_repositories(count)
        logger.info(f"Collected {len(repositories)} repositories")
        
        # Transform to bronze format
        bronze_records = self.transform_to_bronze_format(repositories)
        logger.info(f"Transformed {len(bronze_records)} records to bronze format")
        
        # Save to local files
        output_file = self.save_bronze_data(bronze_records, output_dir)
        
        logger.info("Sample data collection completed")
        logger.debug(f"Data saved to: {output_file}")
        logger.info("Ready for bronze to silver transformation testing")
        
        return output_file


def main():
    """Main entry point for sample data collection."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    try:
        collector = GitHubDataCollector()
        output_file = collector.collect_sample_data(count=100)  # Collect 100 repos for testing
        
        print("Sample data collection completed!")
        print(f"Test data saved to: {output_file}")
        print("Next steps:")
        print("1. Upload this test data to your Bronze lakehouse")
        print("2. Run the bronze→silver transformation notebook")
        print("3. Verify the Silver layer data looks correct")
        
    except Exception as e:
        logger.error(f"Sample data collection failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())