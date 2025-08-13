"""
Microsoft Fabric Notebook Deployment Script
Deploys local notebook files to Microsoft Fabric workspace via REST API
"""

import os
import sys
import json
import base64
import requests
import msal
from typing import Dict, List, Optional
import time

class FabricNotebookDeployer:
    def __init__(self):
        self.tenant_id = os.getenv("FABRIC_TENANT_ID")
        self.client_id = os.getenv("FABRIC_CLIENT_ID") 
        self.client_secret = os.getenv("FABRIC_CLIENT_SECRET")
        self.workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
        
        if not all([self.tenant_id, self.client_id, self.client_secret, self.workspace_id]):
            raise ValueError("Missing required environment variables for Fabric authentication")
            
        self.token = None
        self.base_url = "https://api.fabric.microsoft.com/v1"
        
        # Get project root directory (navigate from src/stacktrend/utils/ to project root)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        
        # Notebook mapping: local file -> Fabric notebook name
        self.notebook_mapping = {
            os.path.join(project_root, "src/stacktrend/notebooks/github_data_ingestion.py"): "GitHub Data Ingestion",
            os.path.join(project_root, "src/stacktrend/notebooks/bronze_to_silver_transformation.py"): "Bronze to Silver Transformation", 
            os.path.join(project_root, "src/stacktrend/notebooks/silver_to_gold_analytics.py"): "Silver to Gold Analytics"
        }
    
    def get_access_token(self) -> str:
        """Get access token for Fabric API"""
        try:
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            result = app.acquire_token_for_client(
                scopes=["https://analysis.windows.net/powerbi/api/.default"]
            )
            
            if "access_token" in result:
                self.token = result["access_token"]
                return self.token
            else:
                raise Exception(f"Failed to acquire token: {result.get('error_description', 'Unknown error')}")
                
        except Exception as e:
            raise Exception(f"Authentication failed: {str(e)}")
    
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authentication"""
        if not self.token:
            self.get_access_token()
            
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def list_notebooks(self) -> List[Dict]:
        """List all notebooks in the workspace"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            return response.json().get("value", [])
        else:
            print(f"Failed to list notebooks: {response.status_code} - {response.text}")
            return []
    
    def find_notebook_by_name(self, name: str) -> Optional[str]:
        """Find notebook ID by name"""
        notebooks = self.list_notebooks()
        
        for notebook in notebooks:
            if notebook.get("displayName") == name:
                return notebook.get("id")
        
        return None
    
    def get_notebook_metadata(self, notebook_id: str) -> dict:
        """Get existing notebook metadata to preserve lakehouse attachments"""
        try:
            url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/getDefinition"
            response = requests.post(url, headers=self.get_headers())
            
            if response.status_code == 200:
                definition = response.json()
                if "definition" in definition and "parts" in definition["definition"]:
                    for part in definition["definition"]["parts"]:
                        if part.get("path") == "notebook-content.ipynb":
                            import base64
                            content = base64.b64decode(part["payload"]).decode('utf-8')
                            notebook_json = json.loads(content)
                            return notebook_json.get("metadata", {})
            return {}
        except Exception as e:
            print(f"Warning: Could not retrieve existing metadata: {e}")
            return {}

    def convert_py_to_notebook_format(self, py_content: str, existing_metadata: dict = None) -> str:
        """Convert Python file to notebook format, preserving existing metadata"""
        # Split content by COMMAND ---------- markers
        cells = []
        current_cell = []
        
        lines = py_content.split('\n')
        
        for line in lines:
            if line.strip() == "# COMMAND ----------":
                if current_cell:
                    cells.append('\n'.join(current_cell))
                    current_cell = []
            elif line.strip().startswith("# MAGIC"):
                # Convert magic commands
                magic_content = line.replace("# MAGIC", "").strip()
                if magic_content.startswith("%md"):
                    current_cell.append(magic_content)
                else:
                    current_cell.append(magic_content)
            else:
                current_cell.append(line)
        
        # Add the last cell
        if current_cell:
            cells.append('\n'.join(current_cell))
        
        # Create notebook structure with preserved metadata
        base_metadata = {
            "language_info": {
                "name": "python"
            }
        }
        
        # Merge with existing metadata to preserve lakehouse attachments
        if existing_metadata:
            base_metadata.update(existing_metadata)
            # Ensure language_info is preserved
            base_metadata["language_info"] = {
                "name": "python"
            }
        
        notebook = {
            "nbformat": 4,
            "nbformat_minor": 2,
            "metadata": base_metadata,
            "cells": []
        }
        
        for cell_content in cells:
            cell_content = cell_content.strip()
            if not cell_content:
                continue
                
            if cell_content.startswith("%md"):
                # Markdown cell
                notebook["cells"].append({
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [cell_content.replace("%md", "").strip()]
                })
            else:
                # Code cell
                notebook["cells"].append({
                    "cell_type": "code", 
                    "metadata": {},
                    "source": [cell_content],
                    "outputs": [],
                    "execution_count": None
                })
        
        return json.dumps(notebook, indent=2)
    
    def update_notebook(self, notebook_id: str, content: str) -> bool:
        """Update existing notebook content while preserving lakehouse attachments"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"
        
        # Get existing notebook to preserve lakehouse attachments
        existing_metadata = self.get_notebook_metadata(notebook_id)
        
        # Convert content to base64
        notebook_content = self.convert_py_to_notebook_format(content, existing_metadata)
        content_b64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
        
        payload = {
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": content_b64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        response = requests.post(url, json=payload, headers=self.get_headers())
        
        if response.status_code in [200, 202]:
            print(f"✅ Successfully updated notebook {notebook_id}")
            return True
        else:
            print(f"Failed to update notebook {notebook_id}: {response.status_code} - {response.text}")
            return False
    
    def create_notebook(self, name: str, content: str) -> Optional[str]:
        """Create new notebook"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        
        # Convert content to base64
        notebook_content = self.convert_py_to_notebook_format(content)
        content_b64 = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
        
        payload = {
            "displayName": name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb", 
                        "payload": content_b64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        response = requests.post(url, json=payload, headers=self.get_headers())
        
        if response.status_code in [201, 202]:
            # For async operations, we might not get the ID immediately
            notebook_id = response.json().get("id") if response.text and response.text != "null" else "async-created"
            print(f"✅ Successfully created notebook '{name}' (Status: {response.status_code})")
            return notebook_id
        else:
            print(f"Failed to create notebook '{name}': {response.status_code} - {response.text}")
            return None
    
    def deploy_notebook(self, file_path: str, notebook_name: str) -> bool:
        """Deploy a single notebook"""
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return False
        
        print(f"Processing {file_path} -> {notebook_name}")
        
        # Read file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if notebook exists
        notebook_id = self.find_notebook_by_name(notebook_name)
        
        if notebook_id:
            # Update existing notebook
            return self.update_notebook(notebook_id, content)
        else:
            # Create new notebook
            new_id = self.create_notebook(notebook_name, content)
            return new_id is not None
    
    def deploy_all(self) -> bool:
        """Deploy all notebooks"""
        print("Starting Fabric notebook deployment...")
        
        success_count = 0
        total_count = len(self.notebook_mapping)
        
        for file_path, notebook_name in self.notebook_mapping.items():
            try:
                if self.deploy_notebook(file_path, notebook_name):
                    success_count += 1
                    time.sleep(2)  # Rate limiting
                else:
                    print(f"Failed to deploy {file_path}")
            except Exception as e:
                print(f"Error deploying {file_path}: {str(e)}")
        
        print("\nDeployment Summary:")
        print(f"Successfully deployed: {success_count}/{total_count} notebooks")
        
        if success_count == total_count:
            print("✅ All notebooks deployed successfully!")
            return True
        else:
            print("⚠️ Some notebooks failed to deploy")
            return False

def main():
    try:
        deployer = FabricNotebookDeployer()
        success = deployer.deploy_all()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"Deployment failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
