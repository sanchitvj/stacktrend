"""
LLM-based Repository Classification System
Uses Azure OpenAI GPT-4o Mini for intelligent repository categorization
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from openai import AsyncAzureOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class RepositoryData:
    """Repository data structure for classification"""
    id: str
    name: str
    full_name: str
    description: Optional[str]
    topics: List[str]
    language: Optional[str]
    stars: int
    

@dataclass
class ClassificationResult:
    """Classification result structure"""
    repo_id: str
    primary_category: str
    subcategory: str
    confidence: float
    classification_timestamp: datetime
    classification_version: str = "llm_v1"


class LLMRepositoryClassifier:
    """
    Azure OpenAI-based repository classifier
    """
    
    def __init__(self, api_key: str, endpoint: str, api_version: str = "2025-01-01-preview", model: str = "gpt-4.1-mini"):
        """Initialize the LLM classifier"""
        self.client = AsyncAzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=endpoint
        )
        self.model = model
        self.batch_size = 10
        self.max_tokens = 2000
        
    def _create_classification_prompt(self, repo_batch: List[RepositoryData]) -> str:
        """Create classification prompt for a batch of repositories"""
        
        prompt = f"""Classify these {len(repo_batch)} GitHub repositories into primary and subcategories.

PRIMARY CATEGORIES (choose exactly one):
- AI: Artificial Intelligence, LLMs, agents, neural networks
- ML: Machine Learning, traditional ML, MLOps, model serving  
- DataEngineering: ETL, pipelines, orchestration, analytics engineering
- Database: SQL, NoSQL, vector databases, graph databases
- WebDev: Frontend, backend, fullstack, mobile development
- DevOps: Containerization, CI/CD, monitoring, infrastructure
- Other: Everything else

SUBCATEGORY EXAMPLES:
- AI: agentic, generative, nlp, computer_vision, llm_tools
- ML: deep_learning, traditional_ml, mlops, model_serving, automl
- DataEngineering: orchestration, streaming, etl, analytics_engineering, data_quality
- Database: sql, nosql, vector_db, graph_db, time_series
- WebDev: frontend, backend, fullstack, mobile, api
- DevOps: containerization, ci_cd, monitoring, infrastructure, security

IMPORTANT RULES:
1. Focus on the repository's PRIMARY purpose, not just the programming language
2. If unclear, use repository name, description, and topics to determine purpose
3. Choose the most specific subcategory that fits
4. Confidence should be 0.0-1.0 (1.0 = completely certain)

Return ONLY a valid JSON object with a "classifications" array:
{{
    "classifications": [
        {{
            "repo_id": "{repo_batch[0].id}",
            "primary_category": "AI",
            "subcategory": "agentic", 
            "confidence": 0.95
        }}
    ]
}}

Repositories to classify:
"""
        
        for i, repo in enumerate(repo_batch, 1):
            topics_str = ", ".join(repo.topics[:5]) if repo.topics else "none"
            description = (repo.description or "no description")[:200]
            
            prompt += f"""
{i}. ID: {repo.id}
   Name: {repo.name}
   Description: {description}
   Topics: [{topics_str}]
   Language: {repo.language or "unknown"}
   Stars: {repo.stars}
"""
        
        prompt += f"""
Return JSON object with classifications array for all {len(repo_batch)} repositories:
{{
    "classifications": [
        // {len(repo_batch)} classification objects here
    ]
}}"""
        return prompt
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _call_llm(self, prompt: str) -> Dict[str, Any]:
        """Make API call to Azure OpenAI with retry logic"""
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a precise repository classifier. Return only valid JSON arrays as requested."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=0.1,  # Low temperature for consistent results
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            parsed_response = json.loads(content)
            logger.debug(f"Parsed LLM response: {parsed_response}")
            return parsed_response
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}, Content: {content}")
            raise
        except Exception as e:
            logger.error(f"LLM API call failed: {e}")
            raise
    
    async def _classify_batch(self, repo_batch: List[RepositoryData]) -> List[ClassificationResult]:
        """Classify a batch of repositories"""
        prompt = self._create_classification_prompt(repo_batch)
        
        try:
            response = await self._call_llm(prompt)
            logger.debug(f"LLM API response: {response}")
            
            # Handle different response formats
            if isinstance(response, dict) and 'classifications' in response:
                classifications = response['classifications']
                logger.debug(f"Found classifications array with {len(classifications)} items")
            elif isinstance(response, list):
                classifications = response
                logger.debug(f"Response is direct array with {len(classifications)} items")
            elif isinstance(response, dict):
                # Check if response contains the array directly
                for key in response:
                    if isinstance(response[key], list):
                        classifications = response[key]
                        logger.debug(f"Found array under key '{key}' with {len(classifications)} items")
                        break
                else:
                    # Single classification result
                    classifications = [response]
                    logger.debug("Single classification result, wrapping in array")
            else:
                classifications = response
                logger.debug(f"Unknown response format: {type(response)}")
            
            results = []
            for i, classification in enumerate(classifications):
                try:
                    # Handle missing keys gracefully
                    repo_id = classification.get('repo_id', repo_batch[i].id if i < len(repo_batch) else f"unknown_{i}")
                    primary_category = classification.get('primary_category', 'Other')
                    subcategory = classification.get('subcategory', 'unknown')
                    confidence = float(classification.get('confidence', 0.1))
                    
                    result = ClassificationResult(
                        repo_id=str(repo_id),
                        primary_category=primary_category,
                        subcategory=subcategory,
                        confidence=confidence,
                        classification_timestamp=datetime.now()
                    )
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing classification {i}: {e}, data: {classification}")
                    # Add fallback result
                    result = ClassificationResult(
                        repo_id=repo_batch[i].id if i < len(repo_batch) else f"unknown_{i}",
                        primary_category="Other",
                        subcategory="unknown",
                        confidence=0.1,
                        classification_timestamp=datetime.now()
                    )
                    results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Batch classification failed: {e}")
            # Return default classifications for the batch
            return [
                ClassificationResult(
                    repo_id=repo.id,
                    primary_category="Other",
                    subcategory="unknown",
                    confidence=0.1,
                    classification_timestamp=datetime.now()
                )
                for repo in repo_batch
            ]
    
    async def _classify_single(self, repo: RepositoryData) -> ClassificationResult:
        """Classify a single repository (fallback method)"""
        try:
            results = await self._classify_batch([repo])
            return results[0] if results else ClassificationResult(
                repo_id=repo.id,
                primary_category="Other",
                subcategory="unknown", 
                confidence=0.1,
                classification_timestamp=datetime.now()
            )
        except Exception as e:
            logger.error(f"Single repository classification failed for {repo.id}: {e}")
            return ClassificationResult(
                repo_id=repo.id,
                primary_category="Other",
                subcategory="unknown",
                confidence=0.1,
                classification_timestamp=datetime.now()
            )
    
    async def classify_repositories(self, repositories: List[RepositoryData]) -> List[ClassificationResult]:
        """
        Main method to classify a list of repositories
        Processes in batches for optimal performance
        """
        if not repositories:
            return []
        
        logger.info(f"Starting classification of {len(repositories)} repositories")
        
        results = []
        batch_size = self.batch_size
        
        for i in range(0, len(repositories), batch_size):
            batch = repositories[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(repositories) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} repositories)")
            
            try:
                batch_results = await self._classify_batch(batch)
                results.extend(batch_results)
                
                # Rate limiting - wait between batches
                if i + batch_size < len(repositories):
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.warning(f"Batch {batch_num} processing failed, falling back to individual processing: {e}")
                
                # Fallback: process individually
                for repo in batch:
                    try:
                        result = await self._classify_single(repo)
                        results.append(result)
                        await asyncio.sleep(0.1)  # Small delay between individual calls
                    except Exception as individual_error:
                        logger.error(f"Failed to classify repo {repo.id}: {individual_error}")
                        # Add default classification
                        results.append(ClassificationResult(
                            repo_id=repo.id,
                            primary_category="Other",
                            subcategory="unknown",
                            confidence=0.1,
                            classification_timestamp=datetime.now()
                        ))
        
        logger.info(f"Classification completed. {len(results)} results generated")
        return results
    
    def classify_repositories_sync(self, repositories: List[RepositoryData]) -> List[ClassificationResult]:
        """Synchronous wrapper for classify_repositories"""
        try:
            # Check if there's already a running event loop
            asyncio.get_running_loop()
            # If we're in a running loop, we can't use asyncio.run()
            # This is a limitation - in Jupyter/async contexts, use the async version
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(self.classify_repositories(repositories))
        except RuntimeError:
            # No running loop, safe to use asyncio.run()
            return asyncio.run(self.classify_repositories(repositories))
        except ImportError:
            # nest_asyncio not available, fallback to basic sync behavior
            logger.warning("Cannot run async classification in sync context. Using fallback classification.")
            # Return fallback classifications
            return [
                ClassificationResult(
                    repo_id=repo.id,
                    primary_category="Other",
                    subcategory="unknown",
                    confidence=0.1,
                    classification_timestamp=datetime.now()
                )
                for repo in repositories
            ]


class ClassificationDriftDetector:
    """Detect and handle classification drift"""
    
    def __init__(self, confidence_threshold: float = 0.3):
        self.confidence_threshold = confidence_threshold
    
    def detect_drift(
        self, 
        old_classifications: List[ClassificationResult], 
        new_classifications: List[ClassificationResult]
    ) -> List[Dict[str, Any]]:
        """
        Detect classification changes that might indicate drift or errors
        """
        changes = []
        
        # Create lookup for old classifications
        old_lookup = {c.repo_id: c for c in old_classifications}
        
        for new_class in new_classifications:
            old_class = old_lookup.get(new_class.repo_id)
            
            if old_class and old_class.primary_category != new_class.primary_category:
                confidence_drop = old_class.confidence - new_class.confidence
                
                change_record = {
                    'repository_id': new_class.repo_id,
                    'old_category': old_class.primary_category,
                    'old_subcategory': old_class.subcategory,
                    'old_confidence': old_class.confidence,
                    'new_category': new_class.primary_category,
                    'new_subcategory': new_class.subcategory,
                    'new_confidence': new_class.confidence,
                    'confidence_change': confidence_drop,
                    'requires_review': confidence_drop > self.confidence_threshold,
                    'timestamp': datetime.now(),
                    'drift_severity': self._calculate_drift_severity(old_class, new_class)
                }
                changes.append(change_record)
        
        return changes
    
    def _calculate_drift_severity(self, old: ClassificationResult, new: ClassificationResult) -> str:
        """Calculate the severity of classification drift"""
        confidence_drop = old.confidence - new.confidence
        
        if confidence_drop > 0.5:
            return "high"
        elif confidence_drop > 0.3:
            return "medium"
        elif old.confidence > 0.9 and new.confidence < 0.7:
            return "medium"
        else:
            return "low"


def create_repository_data_from_dict(repo_dict: Dict[str, Any]) -> RepositoryData:
    """Helper function to create RepositoryData from dictionary"""
    return RepositoryData(
        id=str(repo_dict.get('repository_id', repo_dict.get('id', ''))),
        name=repo_dict.get('name', ''),
        full_name=repo_dict.get('full_name', ''),
        description=repo_dict.get('description') or repo_dict.get('description_clean'),
        topics=repo_dict.get('topics', []) or repo_dict.get('topics_standardized', []),
        language=repo_dict.get('language') or repo_dict.get('primary_language'),
        stars=int(repo_dict.get('stargazers_count', 0) or repo_dict.get('stars', 0))
    )
