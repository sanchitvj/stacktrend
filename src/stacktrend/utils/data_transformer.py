"""
Data transformation utilities for converting bronze layer data to silver layer.
"""

import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging


class SilverTransformer:
    """Transforms raw GitHub repository data into clean silver layer format."""
    
    def __init__(self):
        """Initialize the transformer with language mappings and cleaning rules."""
        self.language_mappings = {
            'javascript': 'JavaScript',
            'js': 'JavaScript',
            'typescript': 'TypeScript',
            'ts': 'TypeScript',
            'python': 'Python',
            'py': 'Python',
            'java': 'Java',
            'golang': 'Go',
            'go': 'Go',
            'rust': 'Rust',
            'rs': 'Rust',
            'cpp': 'C++',
            'c++': 'C++',
            'cplus': 'C++',
            'general': 'Multi-Language'
        }
        
        self.excluded_languages = {'null', 'none', '', None}
        
    def transform_repositories(self, repositories: List[Dict[Any, Any]], metadata: Dict[Any, Any]) -> List[Dict[Any, Any]]:
        """
        Transform a list of repositories from bronze to silver format.
        
        Args:
            repositories: List of raw repository data
            metadata: Collection metadata
            
        Returns:
            List of transformed repository data
        """
        if not repositories:
            logging.info("No repositories provided for transformation")
            return []
            
        logging.info(f"Starting transformation of {len(repositories)} repositories")
        
        # Log data structure for debugging
        if repositories:
            first_item = repositories[0]
            logging.info(f"First repository type: {type(first_item)}")
            if isinstance(first_item, dict):
                logging.info(f"First repository keys: {list(first_item.keys())[:10]}")
            elif isinstance(first_item, str):
                logging.info(f"First repository string: {first_item[:100]}")
        
        transformed_repos = []
        seen_repos = set()  # For deduplication
        successful_transforms = 0
        
        for i, repo in enumerate(repositories):
            try:
                transformed_repo = self._transform_single_repository(repo, metadata)
                
                if transformed_repo:
                    # Deduplicate by full_name
                    repo_id = transformed_repo.get('full_name', '')
                    if repo_id and repo_id not in seen_repos:
                        transformed_repos.append(transformed_repo)
                        seen_repos.add(repo_id)
                        successful_transforms += 1
                        
            except Exception as e:
                repo_name = repo.get('name', 'unknown') if isinstance(repo, dict) else str(repo)[:50]
                logging.warning(f"Failed to transform repository {repo_name}: {str(e)}")
                continue
                
        logging.info(f"Successfully transformed {successful_transforms} repositories out of {len(repositories)} (deduplicated to {len(transformed_repos)})")
        return transformed_repos
    
    def _transform_single_repository(self, repo: Dict[Any, Any], metadata: Dict[Any, Any]) -> Optional[Dict[Any, Any]]:
        """Transform a single repository to silver format."""
        if not repo:
            return None
        
        # Handle case where repo is a string instead of dict
        if isinstance(repo, str):
            logging.warning(f"Repository data is string instead of dict: {repo[:100]}...")
            return None
            
        if not isinstance(repo, dict):
            logging.warning(f"Repository data is not a dict: {type(repo)}")
            return None
            
        # Extract and clean basic fields
        full_name = repo.get('full_name', '').strip()
        if not full_name:
            return None
            
        # Parse dates
        created_at = self._parse_date(repo.get('created_at'))
        updated_at = self._parse_date(repo.get('updated_at'))
        pushed_at = self._parse_date(repo.get('pushed_at'))
        
        # Normalize language
        raw_language = repo.get('language', '').lower() if repo.get('language') else ''
        normalized_language = self._normalize_language(raw_language)
        collection_language = self._normalize_language(repo.get('collection_language', ''))
        
        # Calculate metrics
        stars = int(repo.get('stars', 0))  # Bronze data uses 'stars' not 'stargazers_count'
        forks = int(repo.get('forks', 0))  # Bronze data uses 'forks' not 'forks_count'
        watchers = int(repo.get('watchers', 0))  # Bronze data uses 'watchers' not 'watchers_count'
        size = int(repo.get('size', 0))
        
        # Calculate derived metrics
        momentum_score = self._calculate_momentum_score(repo, created_at)
        activity_score = self._calculate_activity_score(repo, updated_at, pushed_at)
        popularity_tier = self._classify_popularity_tier(stars, forks)
        
        # Build transformed repository
        transformed_repo = {
            # Basic Information
            'full_name': full_name,
            'name': repo.get('name', '').strip(),
            'owner': repo.get('owner', '').strip(),  # Bronze data has 'owner' as string
            'description': self._clean_description(repo.get('description', '')),
            'url': repo.get('url', ''),
            'clone_url': repo.get('clone_url', ''),
            
            # Language Information
            'primary_language': normalized_language,
            'collection_language': collection_language,
            'language_category': self._get_language_category(normalized_language),
            
            # Metrics
            'stars': stars,
            'forks': forks,
            'watchers': watchers,
            'size_kb': size,
            'open_issues': int(repo.get('open_issues', 0)),  # Bronze data uses 'open_issues' not 'open_issues_count'
            
            # Derived Metrics
            'momentum_score': momentum_score,
            'activity_score': activity_score,
            'popularity_tier': popularity_tier,
            'fork_ratio': round(forks / max(stars, 1), 3),
            
            # Dates
            'created_at': created_at.isoformat() if created_at else None,
            'updated_at': updated_at.isoformat() if updated_at else None,
            'pushed_at': pushed_at.isoformat() if pushed_at else None,
            
            # Repository Properties
            'is_fork': bool(repo.get('fork', False)),  # May not exist in bronze data
            'is_archived': bool(repo.get('archived', False)),
            'is_private': repo.get('visibility', 'public') != 'public',  # Bronze data uses 'visibility'  
            'default_branch': repo.get('default_branch', 'main'),
            'has_wiki': bool(repo.get('has_wiki', False)),
            'has_issues': bool(repo.get('has_issues', False)),
            'has_projects': bool(repo.get('has_projects', False)),
            
            # Topics and Keywords
            'topics': repo.get('topics', [])[:10],  # Limit to first 10 topics
            'keywords': self._extract_keywords(repo.get('description', ''), repo.get('topics', [])),
            
            # Processing Metadata
            'collection_timestamp': repo.get('collection_timestamp'),
            'processing_timestamp': datetime.utcnow().isoformat(),
            'data_source': 'github_api',
            'transformation_version': '1.0'
        }
        
        return transformed_repo
    
    def _normalize_language(self, language: str) -> str:
        """Normalize language names to consistent format."""
        if not language or language.lower() in self.excluded_languages:
            return 'Unknown'
            
        language_clean = language.lower().strip()
        return self.language_mappings.get(language_clean, language.title())
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse ISO date string to datetime object."""
        if not date_str:
            return None
            
        try:
            # Handle GitHub's ISO format
            if date_str.endswith('Z'):
                date_str = date_str[:-1] + '+00:00'
            return datetime.fromisoformat(date_str)
        except (ValueError, TypeError):
            return None
    
    def _calculate_momentum_score(self, repo: Dict[Any, Any], created_at: Optional[datetime]) -> float:
        """Calculate repository momentum based on stars per day since creation."""
        if not created_at:
            return 0.0
            
        stars = int(repo.get('stars', 0))  # Use 'stars' from bronze data
        days_old = (datetime.now(timezone.utc) - created_at).days
        
        if days_old <= 0:
            return 0.0
            
        momentum = stars / days_old
        return round(momentum, 4)
    
    def _calculate_activity_score(self, repo: Dict[Any, Any], updated_at: Optional[datetime], pushed_at: Optional[datetime]) -> float:
        """Calculate repository activity score based on recent updates."""
        now = datetime.now(timezone.utc)
        score = 0.0
        
        # Recent push activity (50% weight)
        if pushed_at:
            days_since_push = (now - pushed_at).days
            if days_since_push <= 7:
                score += 50
            elif days_since_push <= 30:
                score += 30
            elif days_since_push <= 90:
                score += 10
        
        # Recent updates (30% weight)
        if updated_at:
            days_since_update = (now - updated_at).days
            if days_since_update <= 7:
                score += 30
            elif days_since_update <= 30:
                score += 20
            elif days_since_update <= 90:
                score += 5
        
        # Open issues ratio (20% weight)
        stars = max(int(repo.get('stars', 0)), 1)  # Use 'stars' from bronze data
        issues = int(repo.get('open_issues', 0))   # Use 'open_issues' from bronze data
        issue_ratio = issues / stars
        
        if issue_ratio < 0.1:
            score += 20
        elif issue_ratio < 0.3:
            score += 10
        
        return round(score, 1)
    
    def _classify_popularity_tier(self, stars: int, forks: int) -> str:
        """Classify repository into popularity tiers."""
        if stars >= 10000:
            return 'viral'
        elif stars >= 5000:
            return 'popular'
        elif stars >= 1000:
            return 'notable'
        elif stars >= 100:
            return 'emerging'
        elif stars >= 10:
            return 'developing'
        else:
            return 'new'
    
    def _get_language_category(self, language: str) -> str:
        """Get broad category for programming language."""
        web_languages = {'JavaScript', 'TypeScript'}
        systems_languages = {'C++', 'Rust', 'Go'}
        scripting_languages = {'Python'}
        enterprise_languages = {'Java'}
        
        if language in web_languages:
            return 'Web Development'
        elif language in systems_languages:
            return 'Systems Programming'
        elif language in scripting_languages:
            return 'Scripting & Data Science'
        elif language in enterprise_languages:
            return 'Enterprise Development'
        else:
            return 'Other'
    
    def _clean_description(self, description: str) -> str:
        """Clean and normalize repository description."""
        if not description:
            return ''
            
        # Remove excessive whitespace and emoji
        cleaned = re.sub(r'\s+', ' ', description.strip())
        
        # Remove common GitHub badges/markdown
        cleaned = re.sub(r'!\[.*?\]\(.*?\)', '', cleaned)  # Remove images
        cleaned = re.sub(r'\[.*?\]\(.*?\)', '', cleaned)   # Remove links
        
        # Limit length
        return cleaned[:500].strip()
    
    def _extract_keywords(self, description: str, topics: List[str]) -> List[str]:
        """Extract relevant keywords from description and topics."""
        keywords = set()
        
        # Add topics
        keywords.update([topic.lower() for topic in topics[:5]])
        
        # Extract keywords from description
        if description:
            # Common tech keywords
            tech_keywords = ['api', 'framework', 'library', 'tool', 'cli', 'web', 'mobile', 
                           'database', 'ml', 'ai', 'data', 'analytics', 'microservice']
            
            description_lower = description.lower()
            for keyword in tech_keywords:
                if keyword in description_lower:
                    keywords.add(keyword)
        
        return list(keywords)[:10]  # Limit to 10 keywords 