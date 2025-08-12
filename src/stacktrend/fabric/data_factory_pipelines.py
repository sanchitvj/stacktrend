"""
Fabric Data Factory Pipeline Configurations
Direct GitHub API ingestion without blob storage intermediate step.
"""

from typing import Dict, Any, List
import json


class FabricDataFactoryPipelines:
    """Configurations for Fabric Data Factory pipelines."""
    
    @staticmethod
    def github_ingestion_pipeline() -> Dict[str, Any]:
        """GitHub API ingestion pipeline configuration."""
        return {
            "name": "github_data_ingestion_direct",
            "description": "Direct GitHub API to Fabric lakehouse ingestion",
            "activities": [
                {
                    "name": "GetTopRepositories",
                    "type": "WebActivity",
                    "method": "GET",
                    "url": "https://api.github.com/search/repositories",
                    "headers": {
                        "Authorization": "token @{linkedService().properties.github_token}",
                        "Accept": "application/vnd.github.v3+json",
                        "User-Agent": "stacktrend-observatory"
                    },
                    "body": {
                        "q": "stars:>1000",
                        "sort": "stars", 
                        "order": "desc",
                        "per_page": 100
                    }
                },
                {
                    "name": "ProcessRepositoryPages",
                    "type": "ForEach",
                    "dependsOn": ["GetTopRepositories"],
                    "items": "@range(1, 10)",  # Process 10 pages = 1000 repos
                    "activities": [
                        {
                            "name": "GetRepositoryPage",
                            "type": "WebActivity", 
                            "method": "GET",
                            "url": "https://api.github.com/search/repositories?q=stars:>1000&sort=stars&order=desc&per_page=100&page=@{item()}",
                            "headers": {
                                "Authorization": "token @{linkedService().properties.github_token}",
                                "Accept": "application/vnd.github.v3+json"
                            }
                        },
                        {
                            "name": "WriteToLakehouse",
                            "type": "Copy",
                            "dependsOn": ["GetRepositoryPage"],
                            "source": {
                                "type": "RestSource",
                                "httpRequestTimeout": "00:01:40",
                                "requestInterval": "00.00:00:02.000"  # Rate limiting
                            },
                            "sink": {
                                "type": "LakehouseSink",
                                "tableName": "bronze_github_repositories",
                                "partitionOption": "DynamicRange",
                                "partitionSettings": {
                                    "partitionColumnNames": ["collection_date"]
                                }
                            }
                        }
                    ]
                }
            ],
            "triggers": [
                {
                    "name": "ScheduledTrigger",
                    "type": "ScheduleTrigger",
                    "recurrence": {
                        "frequency": "Hour",
                        "interval": 6,
                        "startTime": "2025-01-01T00:00:00Z"
                    }
                }
            ]
        }
    
    @staticmethod
    def bronze_to_silver_pipeline() -> Dict[str, Any]:
        """Bronze to Silver transformation pipeline."""
        return {
            "name": "bronze_to_silver_transformation",
            "description": "Transform raw GitHub data to clean Silver layer",
            "activities": [
                {
                    "name": "RunNotebook",
                    "type": "Notebook",
                    "notebook": {
                        "referenceName": "bronze_to_silver_transformation",
                        "type": "NotebookReference"
                    },
                    "executorSize": "Small",
                    "conf": {
                        "spark.dynamicAllocation.enabled": "false",
                        "spark.dynamicAllocation.minExecutors": "1",
                        "spark.dynamicAllocation.maxExecutors": "2"
                    }
                }
            ],
            "triggers": [
                {
                    "name": "TriggerAfterBronze",
                    "type": "TumblingWindowTrigger",
                    "dependsOn": [
                        {
                            "pipelineName": "github_data_ingestion_direct",
                            "type": "PipelineReference"
                        }
                    ],
                    "frequency": "Hour",
                    "interval": 6,
                    "delay": "00:30:00"  # Wait 30 minutes after bronze completes
                }
            ]
        }
    
    @staticmethod
    def silver_to_gold_pipeline() -> Dict[str, Any]:
        """Silver to Gold analytics pipeline."""
        return {
            "name": "silver_to_gold_analytics",
            "description": "Generate analytics and metrics for Gold layer",
            "activities": [
                {
                    "name": "RunAnalyticsNotebook",
                    "type": "Notebook", 
                    "notebook": {
                        "referenceName": "silver_to_gold_analytics",
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
                    "name": "RefreshPowerBIDataset",
                    "type": "WebActivity",
                    "dependsOn": ["RunAnalyticsNotebook"],
                    "method": "POST",
                    "url": "https://api.powerbi.com/v1.0/myorg/groups/@{linkedService().properties.powerbi_workspace_id}/datasets/@{linkedService().properties.powerbi_dataset_id}/refreshes",
                    "headers": {
                        "Authorization": "Bearer @{linkedService().properties.powerbi_token}"
                    }
                }
            ],
            "triggers": [
                {
                    "name": "TriggerAfterSilver",
                    "type": "TumblingWindowTrigger",
                    "dependsOn": [
                        {
                            "pipelineName": "bronze_to_silver_transformation",
                            "type": "PipelineReference"
                        }
                    ],
                    "frequency": "Hour",
                    "interval": 6,
                    "delay": "00:15:00"
                }
            ]
        }
    
    @staticmethod
    def get_linked_services() -> List[Dict[str, Any]]:
        """Required linked services for the pipelines."""
        return [
            {
                "name": "github_api_service",
                "type": "RestService",
                "properties": {
                    "baseUrl": "https://api.github.com",
                    "enableServerCertificateValidation": True,
                    "authentication": {
                        "type": "Anonymous"
                    },
                    "github_token": "@{linkedService().github_token}"  # From Key Vault
                }
            },
            {
                "name": "bronze_lakehouse_service",
                "type": "Lakehouse",
                "properties": {
                    "workspaceId": "@{linkedService().fabric_workspace_id}",
                    "artifactId": "@{linkedService().bronze_lakehouse_id}"
                }
            },
            {
                "name": "silver_lakehouse_service", 
                "type": "Lakehouse",
                "properties": {
                    "workspaceId": "@{linkedService().fabric_workspace_id}",
                    "artifactId": "@{linkedService().silver_lakehouse_id}"
                }
            },
            {
                "name": "gold_lakehouse_service",
                "type": "Lakehouse", 
                "properties": {
                    "workspaceId": "@{linkedService().fabric_workspace_id}",
                    "artifactId": "@{linkedService().gold_lakehouse_id}"
                }
            }
        ]


def generate_pipeline_json_files():
    """Generate JSON files for importing into Fabric Data Factory."""
    pipelines = FabricDataFactoryPipelines()
    
    configs = {
        "github_ingestion": pipelines.github_ingestion_pipeline(),
        "bronze_to_silver": pipelines.bronze_to_silver_pipeline(), 
        "silver_to_gold": pipelines.silver_to_gold_pipeline(),
        "linked_services": pipelines.get_linked_services()
    }
    
    import os
    os.makedirs("fabric_pipelines", exist_ok=True)
    
    for name, config in configs.items():
        with open(f"fabric_pipelines/{name}.json", 'w') as f:
            json.dump(config, f, indent=2)
    
    print("✅ Generated Fabric Data Factory pipeline configurations")
    print("📁 Files created in fabric_pipelines/ directory")
    print("📝 Import these into your Fabric workspace Data Factory")


if __name__ == "__main__":
    generate_pipeline_json_files()