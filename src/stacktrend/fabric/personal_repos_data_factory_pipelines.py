"""
Fabric Data Factory Pipeline Configurations for Personal GitHub Repository Monitoring
Simplified pipeline for monitoring personal/organization repositories for dashboards.
"""

from typing import Dict, Any, List
import json


class PersonalReposDataFactoryPipelines:
    """Configurations for Personal GitHub Repository Monitoring Fabric Data Factory pipelines."""
    
    @staticmethod
    def personal_repos_ingestion_pipeline() -> Dict[str, Any]:
        """Personal GitHub Repository ingestion pipeline configuration."""
        return {
            "name": "personal_github_repos_ingestion",
            "description": "Personal GitHub repository and activity data ingestion pipeline",
            "activities": [
                {
                    "name": "IngestPersonalRepos", 
                    "type": "Notebook",
                    "notebook": {
                        "referenceName": "personal_repos_ingestion",
                        "type": "NotebookReference"
                    },
                    "parameters": {
                        "github_token": "@{linkedService().properties.github_token}",
                        "github_username": "@{variables('github_username')}",
                        "include_private": "@{variables('include_private_repos')}"
                    },
                    "executorSize": "Medium",
                    "conf": {
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": "1",
                        "spark.dynamicAllocation.maxExecutors": "3"
                    }
                }
            ],
            "variables": {
                "github_username": {
                    "type": "String",
                    "defaultValue": ""
                },
                "include_private_repos": {
                    "type": "Boolean",
                    "defaultValue": False
                }
            },
            "triggers": [
                {
                    "name": "DailyPersonalReposIngestion",
                    "type": "TumblingWindowTrigger",
                    "frequency": "Day",
                    "interval": 1,
                    "startTime": "2024-01-01T02:00:00Z",
                    "delay": "00:00:00"
                }
            ]
        }
    
    @staticmethod
    def personal_repos_bronze_to_silver_pipeline() -> Dict[str, Any]:
        """Personal repos Bronze to Silver transformation pipeline."""
        return {
            "name": "personal_repos_bronze_to_silver_transformation",
            "description": "Transform raw personal repository data to clean Silver layer with LLM classification",
            "activities": [
                {
                    "name": "RunPersonalReposTransformation",
                    "type": "Notebook",
                    "notebook": {
                        "referenceName": "personal_repos_bronze_to_silver",
                        "type": "NotebookReference"
                    },
                    "parameters": {
                        "azure_openai_api_key": "@{linkedService().properties.azure_openai_api_key}",
                        "azure_openai_endpoint": "@{linkedService().properties.azure_openai_endpoint}",
                        "azure_openai_api_version": "@{linkedService().properties.azure_openai_api_version}",
                        "azure_openai_model": "@{linkedService().properties.azure_openai_model}"
                    },
                    "executorSize": "Medium",
                    "conf": {
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": "1",
                        "spark.dynamicAllocation.maxExecutors": "4"
                    }
                }
            ],
            "triggers": [
                {
                    "name": "TriggerAfterPersonalReposIngestion",
                    "type": "TumblingWindowTrigger",
                    "dependsOn": [
                        {
                            "pipelineName": "personal_github_repos_ingestion",
                            "type": "PipelineReference"
                        }
                    ],
                    "frequency": "Day",
                    "interval": 1,
                    "delay": "00:30:00"
                }
            ]
        }
    
    @staticmethod
    def personal_repos_silver_to_gold_pipeline() -> Dict[str, Any]:
        """Personal repos Silver to Gold analytics pipeline."""
        return {
            "name": "personal_repos_silver_to_gold_analytics",
            "description": "Generate personal repository analytics for Gold layer and dashboard",
            "activities": [
                {
                    "name": "RunPersonalReposAnalytics",
                    "type": "Notebook",
                    "notebook": {
                        "referenceName": "personal_repos_silver_to_gold",
                        "type": "NotebookReference"
                    },
                    "executorSize": "Medium",
                    "conf": {
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.dynamicAllocation.minExecutors": "1",
                        "spark.dynamicAllocation.maxExecutors": "4"
                    }
                },
                {
                    "name": "RefreshPersonalReposPowerBIDataset",
                    "type": "WebActivity",
                    "dependsOn": [
                        {
                            "activity": "RunPersonalReposAnalytics",
                            "dependencyConditions": ["Succeeded"]
                        }
                    ],
                    "method": "POST",
                    "url": "https://api.powerbi.com/v1.0/myorg/groups/@{linkedService().properties.powerbi_workspace_id}/datasets/@{linkedService().properties.powerbi_personal_repos_dataset_id}/refreshes",
                    "headers": {
                        "Authorization": "Bearer @{linkedService().properties.powerbi_token}",
                        "Content-Type": "application/json"
                    },
                    "body": {
                        "type": "full"
                    }
                }
            ],
            "triggers": [
                {
                    "name": "TriggerAfterPersonalReposSilver",
                    "type": "TumblingWindowTrigger",
                    "dependsOn": [
                        {
                            "pipelineName": "personal_repos_bronze_to_silver_transformation",
                            "type": "PipelineReference"
                        }
                    ],
                    "frequency": "Day",
                    "interval": 1,
                    "delay": "00:30:00"
                }
            ]
        }
    
    @staticmethod
    def personal_repos_master_pipeline() -> Dict[str, Any]:
        """Master pipeline orchestrating all personal repository analysis."""
        return {
            "name": "personal_repos_master_pipeline",
            "description": "Master orchestration pipeline for personal GitHub repository monitoring",
            "activities": [
                {
                    "name": "ExecutePersonalReposIngestion",
                    "type": "ExecutePipeline",
                    "pipeline": {
                        "referenceName": "personal_github_repos_ingestion",
                        "type": "PipelineReference"
                    },
                    "waitOnCompletion": True
                },
                {
                    "name": "ExecutePersonalReposTransformation",
                    "type": "ExecutePipeline",
                    "dependsOn": [
                        {
                            "activity": "ExecutePersonalReposIngestion",
                            "dependencyConditions": ["Succeeded"]
                        }
                    ],
                    "pipeline": {
                        "referenceName": "personal_repos_bronze_to_silver_transformation",
                        "type": "PipelineReference"
                    },
                    "waitOnCompletion": True
                },
                {
                    "name": "ExecutePersonalReposAnalytics",
                    "type": "ExecutePipeline",
                    "dependsOn": [
                        {
                            "activity": "ExecutePersonalReposTransformation",
                            "dependencyConditions": ["Succeeded"]
                        }
                    ],
                    "pipeline": {
                        "referenceName": "personal_repos_silver_to_gold_analytics",
                        "type": "PipelineReference"
                    },
                    "waitOnCompletion": True
                }
            ],
            "triggers": [
                {
                    "name": "DailyPersonalReposMonitoring",
                    "type": "TumblingWindowTrigger",
                    "frequency": "Day",
                    "interval": 1,
                    "startTime": "2024-01-01T01:00:00Z",
                    "delay": "00:00:00"
                }
            ]
        }
    
    @staticmethod
    def get_personal_repos_linked_services() -> List[Dict[str, Any]]:
        """Required linked services for personal repository monitoring pipelines."""
        return [
            {
                "name": "personal_github_api_service",
                "type": "RestService",
                "properties": {
                    "baseUrl": "https://api.github.com",
                    "enableServerCertificateValidation": True,
                    "authentication": {
                        "type": "Anonymous"
                    },
                    "github_token": "@{linkedService().github_token}",  # From Key Vault
                    "azure_openai_api_key": "@{linkedService().azure_openai_api_key}",
                    "azure_openai_endpoint": "@{linkedService().azure_openai_endpoint}",
                    "azure_openai_api_version": "@{linkedService().azure_openai_api_version}",
                    "azure_openai_model": "@{linkedService().azure_openai_model}"
                }
            },
            {
                "name": "personal_bronze_lakehouse_service",
                "type": "Lakehouse",
                "properties": {
                    "workspaceId": "@{linkedService().fabric_workspace_id}",
                    "artifactId": "@{linkedService().bronze_lakehouse_id}"
                }
            },
            {
                "name": "personal_silver_lakehouse_service", 
                "type": "Lakehouse",
                "properties": {
                    "workspaceId": "@{linkedService().fabric_workspace_id}",
                    "artifactId": "@{linkedService().silver_lakehouse_id}"
                }
            },
            {
                "name": "personal_gold_lakehouse_service",
                "type": "Lakehouse", 
                "properties": {
                    "workspaceId": "@{linkedService().fabric_workspace_id}",
                    "artifactId": "@{linkedService().gold_lakehouse_id}"
                }
            },
            {
                "name": "personal_powerbi_service",
                "type": "PowerBI",
                "properties": {
                    "workspaceId": "@{linkedService().powerbi_workspace_id}",
                    "powerbi_token": "@{linkedService().powerbi_token}",
                    "powerbi_personal_repos_dataset_id": "@{linkedService().powerbi_personal_repos_dataset_id}"
                }
            }
        ]


def generate_personal_repos_pipeline_json_files():
    """Generate JSON files for importing Personal Repository pipelines into Fabric Data Factory."""
    pipelines = PersonalReposDataFactoryPipelines()
    
    configs = {
        "personal_repos_ingestion": pipelines.personal_repos_ingestion_pipeline(),
        "personal_repos_bronze_to_silver": pipelines.personal_repos_bronze_to_silver_pipeline(),
        "personal_repos_silver_to_gold": pipelines.personal_repos_silver_to_gold_pipeline(),
        "personal_repos_master_pipeline": pipelines.personal_repos_master_pipeline(),
        "personal_repos_linked_services": pipelines.get_personal_repos_linked_services()
    }
    
    import os
    os.makedirs("fabric_personal_repos_pipelines", exist_ok=True)
    
    for name, config in configs.items():
        with open(f"fabric_personal_repos_pipelines/{name}.json", 'w') as f:
            json.dump(config, f, indent=2)
    
    print("✅ Generated Personal Repository Fabric Data Factory pipeline configurations")
    print("📁 Files created in fabric_personal_repos_pipelines/ directory")
    print("📝 Import these into your Fabric workspace Data Factory")
    print("\n📋 Pipeline Import Order:")
    print("1. personal_repos_linked_services.json - Import linked services first")
    print("2. personal_repos_ingestion.json - Import ingestion pipeline")
    print("3. personal_repos_bronze_to_silver.json - Import transformation pipeline")
    print("4. personal_repos_silver_to_gold.json - Import analytics pipeline")
    print("5. personal_repos_master_pipeline.json - Import master orchestration pipeline")
    
    print("\n🔧 Configuration Steps:")
    print("1. Update linked service credentials in Fabric")
    print("2. Set your GitHub username in pipeline variables")
    print("3. Configure Azure OpenAI credentials (same as main pipeline)")
    print("4. Set up Power BI workspace and dataset IDs")
    print("5. Test individual pipelines before enabling triggers")
    
    print(f"\n🎯 Focus: Personal repository monitoring for {3} core tables:")
    print("   • github_my_portfolio (Silver) - Your classified repositories")
    print("   • portfolio_overview (Gold) - Executive dashboard KPIs")
    print("   • repo_health_dashboard (Gold) - Individual repository health")


if __name__ == "__main__":
    generate_personal_repos_pipeline_json_files()
