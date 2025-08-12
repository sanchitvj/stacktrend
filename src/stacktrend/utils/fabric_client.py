"""Microsoft Fabric API client for managing lakehouses, pipelines, and datasets."""

import requests
from typing import Dict, List, Any
from datetime import datetime, timedelta
import logging
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class FabricClient:
    """Client for interacting with Microsoft Fabric APIs."""
    
    def __init__(self, settings: Settings = None):
        """Initialize Fabric client with authentication."""
        self.settings = settings or Settings()
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self.access_token = None
        self.token_expires_at = None
        
    def _get_access_token(self) -> str:
        """Get or refresh Azure AD access token for Fabric."""
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token
            
        auth_config = self.settings.get_fabric_auth_config()
        
        # Azure AD token endpoint
        token_url = f"https://login.microsoftonline.com/{auth_config['tenant_id']}/oauth2/v2.0/token"
        
        token_data = {
            "grant_type": "client_credentials",
            "client_id": auth_config["client_id"],
            "client_secret": auth_config["client_secret"],
            "scope": "https://api.fabric.microsoft.com/.default"
        }
        
        try:
            response = requests.post(token_url, data=token_data)
            response.raise_for_status()
            
            token_info = response.json()
            self.access_token = token_info["access_token"]
            # Set expiration with 5-minute buffer
            expires_in = token_info.get("expires_in", 3600)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)
            
            logger.debug("Fabric access token obtained")
            return self.access_token
            
        except requests.RequestException as e:
            logger.error(f"Failed to obtain Fabric access token: {e}")
            raise
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated request to Fabric API."""
        headers = kwargs.get("headers", {})
        headers["Authorization"] = f"Bearer {self._get_access_token()}"
        headers["Content-Type"] = "application/json"
        kwargs["headers"] = headers
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"Fabric API request failed: {method} {url} - {e}")
            raise
    
    def list_lakehouses(self, workspace_id: str = None) -> List[Dict[str, Any]]:
        """List all lakehouses in the workspace."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/lakehouses"
        
        response = self._make_request("GET", endpoint)
        return response.json().get("value", [])
    
    def create_lakehouse(self, name: str, workspace_id: str = None) -> Dict[str, Any]:
        """Create a new lakehouse in the workspace."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/lakehouses"
        
        payload = {
            "displayName": name,
            "description": f"Lakehouse for {name} layer of technology adoption observatory"
        }
        
        response = self._make_request("POST", endpoint, json=payload)
        logger.info(f"Created lakehouse: {name}")
        return response.json()
    
    def get_lakehouse(self, lakehouse_id: str, workspace_id: str = None) -> Dict[str, Any]:
        """Get details of a specific lakehouse."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        
        response = self._make_request("GET", endpoint)
        return response.json()
    
    def list_notebooks(self, workspace_id: str = None) -> List[Dict[str, Any]]:
        """List all notebooks in the workspace."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/notebooks"
        
        response = self._make_request("GET", endpoint)
        return response.json().get("value", [])
    
    def create_notebook(self, name: str, workspace_id: str = None) -> Dict[str, Any]:
        """Create a new notebook in the workspace."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/notebooks"
        
        payload = {
            "displayName": name,
            "description": f"Data processing notebook: {name}"
        }
        
        response = self._make_request("POST", endpoint, json=payload)
        logger.info(f"Created notebook: {name}")
        return response.json()
    
    def list_data_pipelines(self, workspace_id: str = None) -> List[Dict[str, Any]]:
        """List all data pipelines in the workspace."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/dataPipelines"
        
        response = self._make_request("GET", endpoint)
        return response.json().get("value", [])
    
    def create_data_pipeline(self, name: str, workspace_id: str = None) -> Dict[str, Any]:
        """Create a new data pipeline in the workspace."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/dataPipelines"
        
        payload = {
            "displayName": name,
            "description": f"Data pipeline: {name}"
        }
        
        response = self._make_request("POST", endpoint, json=payload)
        logger.info(f"Created data pipeline: {name}")
        return response.json()
    
    def trigger_pipeline_run(self, pipeline_id: str, workspace_id: str = None) -> Dict[str, Any]:
        """Trigger a pipeline run."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/dataPipelines/{pipeline_id}/jobs/instances"
        
        payload = {"jobType": "Pipeline"}
        
        response = self._make_request("POST", endpoint, json=payload)
        logger.info(f"Triggered pipeline run: {pipeline_id}")
        return response.json()
    
    def get_pipeline_run_status(self, pipeline_id: str, run_id: str, workspace_id: str = None) -> Dict[str, Any]:
        """Get the status of a pipeline run."""
        workspace_id = workspace_id or self.settings.FABRIC_WORKSPACE_ID
        endpoint = f"workspaces/{workspace_id}/dataPipelines/{pipeline_id}/jobs/instances/{run_id}"
        
        response = self._make_request("GET", endpoint)
        return response.json()
    
    def setup_workspace_resources(self) -> Dict[str, List[Dict[str, Any]]]:
        """Set up all required Fabric resources for the project."""
        logger.info("Setting up Fabric workspace resources...")
        
        results = {
            "lakehouses": [],
            "notebooks": [],
            "pipelines": []
        }
        
        # Create lakehouses for medallion architecture
        lakehouse_names = [
            self.settings.BRONZE_LAKEHOUSE_NAME,
            self.settings.SILVER_LAKEHOUSE_NAME, 
            self.settings.GOLD_LAKEHOUSE_NAME
        ]
        
        existing_lakehouses = {lh["displayName"]: lh for lh in self.list_lakehouses()}
        
        for name in lakehouse_names:
            if name not in existing_lakehouses:
                lakehouse = self.create_lakehouse(name)
                results["lakehouses"].append(lakehouse)
                logger.info(f"Created lakehouse: {name}")
            else:
                logger.debug(f"Lakehouse already exists: {name}")
                results["lakehouses"].append(existing_lakehouses[name])
        
        # Create notebooks for data processing
        notebook_names = [
            "bronze_to_silver_transformation",
            "silver_to_gold_analytics", 
            "technology_taxonomy_mapping",
            "data_quality_validation"
        ]
        
        existing_notebooks = {nb["displayName"]: nb for nb in self.list_notebooks()}
        
        for name in notebook_names:
            if name not in existing_notebooks:
                notebook = self.create_notebook(name)
                results["notebooks"].append(notebook)
                logger.info(f"Created notebook: {name}")
            else:
                logger.debug(f"Notebook already exists: {name}")
                results["notebooks"].append(existing_notebooks[name])
        
        # Create data pipelines
        pipeline_names = [
            self.settings.GITHUB_INGESTION_PIPELINE,
            self.settings.BRONZE_TO_SILVER_PIPELINE,
            self.settings.SILVER_TO_GOLD_PIPELINE
        ]
        
        existing_pipelines = {dp["displayName"]: dp for dp in self.list_data_pipelines()}
        
        for name in pipeline_names:
            if name not in existing_pipelines:
                pipeline = self.create_data_pipeline(name)
                results["pipelines"].append(pipeline)
                logger.info(f"Created pipeline: {name}")
            else:
                logger.debug(f"Pipeline already exists: {name}")
                results["pipelines"].append(existing_pipelines[name])
        
        logger.info("Fabric workspace setup completed")
        return results


# Convenience instance
fabric_client = FabricClient()