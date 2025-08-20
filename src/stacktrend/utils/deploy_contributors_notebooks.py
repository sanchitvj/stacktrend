"""
Microsoft Fabric Contributors Notebooks Deployment Script
Deploys contributors-focused notebook files to Microsoft Fabric workspace via REST API
"""

import os
import sys
import base64
import requests
import msal
from typing import Dict, List, Optional
import time

class FabricContributorsNotebookDeployer:
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
        
        # Personal repos notebook mapping: local file -> Fabric notebook name
        self.notebook_mapping = {
            os.path.join(project_root, "src/stacktrend/notebooks/personal_repos_ingestion.py"): "Personal Repos Ingestion",
            os.path.join(project_root, "src/stacktrend/notebooks/personal_repos_bronze_to_silver.py"): "Personal Repos Bronze to Silver",
            os.path.join(project_root, "src/stacktrend/notebooks/personal_repos_silver_to_gold.py"): "Personal Repos Silver to Gold"
        }
    
    def get_access_token(self) -> str:
        """Get access token for Fabric API"""
        try:
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}"
            )
            
            result = app.acquire_token_for_client(scopes=["https://api.fabric.microsoft.com/.default"])
            
            if "access_token" in result:
                return result["access_token"]
            else:
                error_description = result.get("error_description", "Unknown authentication error")
                raise Exception(f"Authentication failed: {error_description}")
                
        except Exception as e:
            raise Exception(f"Failed to get access token: {e}")
    
    def ensure_authentication(self):
        """Ensure we have a valid access token"""
        if not self.token:
            self.token = self.get_access_token()
            print("✅ Successfully authenticated with Fabric API")
    
    def get_headers(self):
        """Get headers for API requests"""
        self.ensure_authentication()
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def convert_py_to_notebook_format(self, py_content: str, existing_metadata: dict = None) -> str:
        """Convert Python file to notebook format, preserving existing metadata"""
        import json
        
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
    
    def get_notebooks(self) -> List[Dict]:
        """Get list of notebooks in the workspace"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            data = response.json()
            return data.get("value", [])
        else:
            print("Failed to get notebooks: {} - {}".format(response.status_code, response.text))
            return []
    
    def find_notebook_by_name(self, name: str) -> Optional[str]:
        """Find notebook ID by name"""
        try:
            notebooks = self.get_notebooks()
            for notebook in notebooks:
                if notebook.get("displayName") == name:
                    return notebook.get("id")
            return None
        except Exception as e:
            print(f"Error finding notebook {name}: {e}")
            return None
    
    def create_notebook(self, name: str, content: str) -> Optional[str]:
        """Create new notebook - using original's fast pattern"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        
        # Convert content to base64 - using original's conversion
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
            print("✅ Successfully created notebook '{}' (Status: {})".format(name, response.status_code))
            return notebook_id
        else:
            print("Failed to create notebook '{}': {} - {}".format(name, response.status_code, response.text))
            return None
    
    def update_notebook(self, notebook_id: str, content: str) -> bool:
        """Update existing notebook content - using original's fast pattern"""
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"
        
        # Convert content to base64 - using original's conversion
        notebook_content = self.convert_py_to_notebook_format(content)
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
            print("✅ Successfully updated notebook {}".format(notebook_id))
            return True
        else:
            print("Failed to update notebook {}: {} - {}".format(notebook_id, response.status_code, response.text))
            return False
    


    def deploy_notebook(self, file_path: str, notebook_name: str) -> bool:
        """Deploy a single notebook - fast and simple like original"""
        if not os.path.exists(file_path):
            print("File not found: {}".format(file_path))
            return False
        
        # Read file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if notebook exists
        notebook_id = self.find_notebook_by_name(notebook_name)
        
        if notebook_id:
            # Update existing notebook
            print("Updating {} -> {}".format(file_path, notebook_name))
            return self.update_notebook(notebook_id, content)
        else:
            # Create new notebook
            print("Creating {} -> {}".format(file_path, notebook_name))
            result = self.create_notebook(notebook_name, content)
            return result is not None
    
    def deploy_all(self) -> bool:
        """Deploy all personal repos notebooks - fast like original"""
        print("Starting Personal Repos Fabric notebook deployment...")
        
        success_count = 0
        total_count = len(self.notebook_mapping)
        
        for file_path, notebook_name in self.notebook_mapping.items():
            try:
                if self.deploy_notebook(file_path, notebook_name):
                    success_count += 1
                    time.sleep(2)  # Short rate limiting like original
                else:
                    print("Failed to deploy {}".format(file_path))
            except Exception as e:
                print("Error deploying {}: {}".format(file_path, str(e)))
        
        print("\nPersonal Repos Deployment Summary:")
        print("Successfully deployed: {}/{} notebooks".format(success_count, total_count))
        
        if success_count == total_count:
            print("✅ All personal repos notebooks deployed successfully!")
            return True
        else:
            print("❌ Some personal repos notebooks failed to deploy")
            return False

def main():
    try:
        deployer = FabricContributorsNotebookDeployer()
        success = deployer.deploy_all()
        
        if not success:
            sys.exit(1)
            
    except Exception as e:
        print("❌ Personal repos deployment failed: {}".format(str(e)))
        sys.exit(1)

if __name__ == "__main__":
    main()
