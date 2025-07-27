"""GitHub API client utilities."""

import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
from github import Github, RateLimitExceededException

from stacktrend.config.settings import settings


class GitHubClient:
    """Client for interacting with GitHub API."""
    
    def __init__(self):
        """Initialize the GitHub client."""
        if not settings.GITHUB_TOKEN:
            raise ValueError("GitHub token not configured")
            
        self.github = Github(settings.GITHUB_TOKEN)
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {settings.GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        })
        
        # Rate limiting tracking
        self.requests_made = 0
        self.last_reset_time = datetime.now()
    
    def get_rate_limit_info(self) -> Dict[str, Any]:
        """Get current rate limit information."""
        try:
            rate_limit = self.github.get_rate_limit()
            return {
                'core': {
                    'limit': rate_limit.core.limit,
                    'remaining': rate_limit.core.remaining,
                    'reset': rate_limit.core.reset,
                    'used': rate_limit.core.used
                },
                'search': {
                    'limit': rate_limit.search.limit,
                    'remaining': rate_limit.search.remaining,
                    'reset': rate_limit.search.reset,
                    'used': rate_limit.search.used
                }
            }
        except Exception as e:
            print(f"❌ Error getting rate limit info: {e}")
            return {}
    
    def wait_for_rate_limit_reset(self):
        """Wait for rate limit to reset if necessary."""
        rate_limit_info = self.get_rate_limit_info()
        
        if rate_limit_info and rate_limit_info['core']['remaining'] < 100:
            reset_time = rate_limit_info['core']['reset']
            wait_time = (reset_time - datetime.now()).total_seconds()
            
            if wait_time > 0:
                print(f"⚠️ Rate limit low. Waiting {wait_time:.0f} seconds...")
                time.sleep(min(wait_time + 60, 3600))  # Max 1 hour wait
    
    def get_trending_repositories(self, language: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trending repositories.
        
        Args:
            language: Filter by programming language
            limit: Maximum number of repositories to return
            
        Returns:
            List of repository data dictionaries
        """
        repos_data = []
        
        try:
            # Search for repositories with recent activity
            query = "stars:>1000 pushed:>2024-01-01"
            if language:
                query += f" language:{language}"
            
            self.wait_for_rate_limit_reset()
            repositories = self.github.search_repositories(query=query, sort="stars", order="desc")
            
            count = 0
            for repo in repositories:
                if count >= limit:
                    break
                    
                try:
                    repo_data = self._extract_repository_data(repo)
                    repos_data.append(repo_data)
                    count += 1
                    
                    # Rate limiting
                    if count % 10 == 0:
                        time.sleep(1)  # Small delay every 10 repos
                        
                except Exception as e:
                    print(f"❌ Error processing repo {repo.full_name}: {e}")
                    continue
            
            print(f"✅ Successfully collected {len(repos_data)} repositories")
            return repos_data
            
        except RateLimitExceededException:
            print("❌ GitHub rate limit exceeded")
            self.wait_for_rate_limit_reset()
            return repos_data
        except Exception as e:
            print(f"❌ Error fetching repositories: {e}")
            return repos_data
    
    def get_repository_details(self, repo_full_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific repository.
        
        Args:
            repo_full_name: Repository name in format "owner/repo"
            
        Returns:
            Dictionary with repository details or None if error
        """
        try:
            self.wait_for_rate_limit_reset()
            repo = self.github.get_repo(repo_full_name)
            return self._extract_repository_data(repo)
            
        except Exception as e:
            print(f"❌ Error fetching repository {repo_full_name}: {e}")
            return None
    
    def _extract_repository_data(self, repo) -> Dict[str, Any]:
        """Extract relevant data from a repository object."""
        try:
            return {
                'id': repo.id,
                'name': repo.name,
                'full_name': repo.full_name,
                'owner': repo.owner.login,
                'description': repo.description,
                'url': repo.html_url,
                'language': repo.language,
                'created_at': repo.created_at.isoformat() if repo.created_at else None,
                'updated_at': repo.updated_at.isoformat() if repo.updated_at else None,
                'pushed_at': repo.pushed_at.isoformat() if repo.pushed_at else None,
                'stars': repo.stargazers_count,
                'watchers': repo.watchers_count,
                'forks': repo.forks_count,
                'open_issues': repo.open_issues_count,
                'subscribers_count': repo.subscribers_count,
                'network_count': repo.network_count,
                'size': repo.size,
                'topics': repo.get_topics() if hasattr(repo, 'get_topics') else [],
                'license': repo.license.name if repo.license else None,
                'has_issues': repo.has_issues,
                'has_projects': repo.has_projects,
                'has_wiki': repo.has_wiki,
                'has_pages': repo.has_pages,
                'archived': repo.archived,
                'disabled': repo.disabled,
                'visibility': getattr(repo, 'visibility', 'public'),
                'default_branch': repo.default_branch,
                'collected_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            print(f"❌ Error extracting repository data: {e}")
            return {'error': str(e), 'collected_at': datetime.utcnow().isoformat()}
    
    def get_contributor_stats(self, repo_full_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get contributor statistics for a repository.
        
        Args:
            repo_full_name: Repository name in format "owner/repo"
            limit: Maximum number of contributors to return
            
        Returns:
            List of contributor data dictionaries
        """
        try:
            self.wait_for_rate_limit_reset()
            repo = self.github.get_repo(repo_full_name)
            contributors = repo.get_contributors()
            
            contributor_data = []
            count = 0
            
            for contributor in contributors:
                if count >= limit:
                    break
                    
                try:
                    contributor_data.append({
                        'login': contributor.login,
                        'id': contributor.id,
                        'contributions': contributor.contributions,
                        'type': contributor.type,
                        'url': contributor.html_url,
                        'collected_at': datetime.utcnow().isoformat()
                    })
                    count += 1
                except Exception as e:
                    print(f"❌ Error processing contributor: {e}")
                    continue
            
            return contributor_data
            
        except Exception as e:
            print(f"❌ Error fetching contributors for {repo_full_name}: {e}")
            return [] 