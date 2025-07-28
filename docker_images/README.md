# Docker Images Directory

This directory contains Dockerfiles for different Azure Functions and services in the StackTrend project.

## Naming Convention

Dockerfiles are named using the pattern: `{function-name}.Dockerfile`

## Current Images

### `github-collector.Dockerfile`
- **Purpose**: GitHub data collector Azure Function
- **Trigger**: Timer-based (every 6 hours)
- **Function**: Collects trending repository data and stores in Azure Storage bronze layer
- **Dependencies**: 
  - `src/stacktrend/utils/github_client.py`
  - `src/stacktrend/utils/azure_client.py`
  - `src/stacktrend/config/settings.py`
  - `github_collector/` function code

## CI/CD Integration

Each Dockerfile has a corresponding GitHub Actions workflow that:
1. Builds the Docker image when relevant code changes
2. Pushes to Azure Container Registry (ACR)
3. Deploys to Azure Container Apps

## Future Functions

When adding new functions, follow this pattern:
1. Create `{function-name}.Dockerfile` in this directory
2. Create corresponding GitHub Actions workflow
3. Update this README with the new function details

## AWS Comparison

This structure is equivalent to having separate Lambda container images for different functions, each with their own ECR repositories and deployment pipelines. 