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
    
    def __init__(self, api_key: str, endpoint: str, api_version: str = "2025-01-01-preview", model: str = "o4-mini"):
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
        
        prompt = f"""You are an expert software engineer analyzing GitHub repositories. Classify these {len(repo_batch)} repositories into technology categories based on their PRIMARY purpose and functionality.

PRIMARY CATEGORIES (choose exactly one):
- AI: Large Language Models, Generative AI, Agentic AI, MCP Servers, Autonomous Agents, AI Infrastructure
- ML: Machine Learning, Deep Learning, MLOps, Data Science, Computer Vision, NLP, Statistical Models
- DataEngineering: Data Pipelines, ETL/ELT, Streaming, DataOps, Data Mesh, Analytics Engineering, Data Quality
- Databases: SQL/NoSQL databases, Vector Stores, Time Series DBs, Graph Databases, Distributed Systems, Caching
- WebDevelopment: Frontend/Backend, Mobile Apps, APIs, Web Frameworks, Serverless, JAMstack, Progressive Web Apps
- DevOps: CI/CD, Infrastructure as Code, Containerization, Monitoring, GitOps, Cloud Engineering
- CloudServices: Cloud Provider Tools, Serverless Platforms, SaaS SDKs, IaaS/PaaS Tools, Multi-Cloud
- Security: Cybersecurity, DevSecOps, Identity Management, Threat Detection, Cryptography, Zero Trust
- ProgrammingLanguages: Language Implementations, Compilers, Interpreters, Language Servers, Build Systems
- Other: General utilities, gaming, system tools, educational content, miscellaneous

SUBCATEGORY GUIDELINES:
- AI: generative_ai, llm_tools, agentic_ai, mcp_servers, autonomous_agents, ai_infrastructure, reinforcement_learning
- ML: deep_learning, machine_learning, mlops, data_science, model_serving, computer_vision, nlp, statistical_models
- DataEngineering: etl, streaming, orchestration, dataops, data_mesh, analytics_engineering, data_quality, real_time_processing
- Databases: relational, nosql, vector_db, time_series, graph_db, distributed_db, caching, in_memory
- WebDevelopment: frontend, backend, fullstack, mobile, api, web_framework, serverless, jamstack, pwa
- DevOps: containerization, ci_cd, monitoring, infrastructure_as_code, gitops, cloud_engineering, observability
- CloudServices: aws_tools, azure_tools, gcp_tools, multi_cloud, serverless_platforms, saas_tools, paas_tools
- Security: cybersecurity, devsecops, identity_management, threat_detection, cryptography, zero_trust, compliance
- ProgrammingLanguages: language_implementation, compilers, interpreters, language_servers, build_tools, package_managers
- Other: utilities, gaming, system_tools, educational, documentation, testing_tools

ENHANCED CLASSIFICATION RULES:
1. Analyze repository NAME, DESCRIPTION, TOPICS, and PRIMARY LANGUAGE to understand core purpose
2. Prioritize the MAIN functionality over supporting technologies (e.g., a web app using ML is WebDevelopment, not ML)
3. Consider modern patterns: AI-first applications, cloud-native architectures, serverless designs
4. Key indicators to look for:
   - AI/ML: model training, inference, transformers, neural networks, AI agents
   - DataEngineering: pipeline, ETL, streaming, kafka, airflow, spark, data lake
   - Security: auth, encryption, security scanning, vulnerability, penetration testing
   - CloudServices: AWS/Azure/GCP in name, terraform, kubernetes operators for cloud
5. Confidence scoring:
   - 0.9+: Clear technology stack and purpose evident
   - 0.8+: Strong indicators with minor ambiguity
   - 0.7+: Reasonable certainty based on available information
   - 0.6+: Educated guess with limited information
6. Avoid "Other" unless truly unclear or doesn't fit established patterns

MODERN TECHNOLOGY INDICATORS:
- Vector databases, embeddings → Databases (vector_db)
- LLM applications, RAG systems → AI (llm_tools)
- Data mesh, DataOps → DataEngineering (data_mesh, dataops)
- GitOps, ArgoCD → DevOps (gitops)
- JAMstack, Edge computing → WebDevelopment (jamstack)
- Zero Trust, SIEM → Security (zero_trust, threat_detection)
- MCP protocol implementations, Model Context Protocol → AI (mcp_servers)

Return ONLY a valid JSON object with a "classifications" array:
{{
    "classifications": [
        {{
            "repo_id": "{repo_batch[0].id}",
            "primary_category": "AI",
            "subcategory": "llm_tools", 
            "confidence": 0.95,
            "reasoning": "Repository implements RAG system with vector embeddings"
        }}
    ]
}}

Repositories to classify:
"""
        
        for i, repo in enumerate(repo_batch, 1):
            topics_str = ", ".join(repo.topics[:5]) if repo.topics else "none"
            description = (repo.description or "no description")[:500]
            
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
                max_completion_tokens=self.max_tokens,  # Updated parameter name for 2025 API
                # temperature=1 is default and only supported value for o4-mini
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            parsed_response = json.loads(content)
            return parsed_response
            
        except json.JSONDecodeError as e:
            print(f"ERROR: JSON decode error: {e}")
            print(f"ERROR: Raw content: {content[:500]}...")
            raise Exception(f"LLM returned invalid JSON: {e}")
        except Exception as e:
            print(f"ERROR: LLM API call failed: {e}")
            print(f"ERROR: Error type: {type(e).__name__}")
            print(f"ERROR: Model: {self.model}")
            print(f"ERROR: Endpoint: {self.client._azure_endpoint}")
            
            # Check for common deployment name issues
            error_str = str(e).lower()
            if "deploymentnotfound" in error_str or "model not found" in error_str or "404" in error_str:
                print("\nPOSSIBLE ISSUE: Model deployment name mismatch!")
                print(f"You're trying to use model: '{self.model}'")
                print("In Azure OpenAI, the 'model' parameter should be your DEPLOYMENT NAME, not the base model name")
                print("Check your Azure OpenAI Studio -> Deployments to see the actual deployment name")
                print("Common examples:")
                print("   - Base model: 'gpt-4o-mini' -> Deployment name might be: 'my-gpt4o-mini', 'gpt4omini', etc.")
                print("   - Set AZURE_OPENAI_MODEL to your actual deployment name, not 'gpt-4o-mini'")
            
            raise Exception(f"Azure OpenAI API error: {e}")
    
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
        except ImportError as e:
            # nest_asyncio not available, fallback to basic sync behavior
            raise Exception(f"LLM classification requires nest_asyncio: {e}")
        except Exception as e:
            raise Exception(f"LLM classification setup failed: {e}")


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
