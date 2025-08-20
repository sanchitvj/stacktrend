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
    
    def get_notebooks(self) -> List[Dict]:
        """Get list of notebooks in the workspace"""
        self.ensure_authentication()
        
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            return data.get("value", [])
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get notebooks: {e}")
    
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
        """Create a new notebook"""
        self.ensure_authentication()
        
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Encode content as base64
        content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        
        payload = {
            "displayName": name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": content_b64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            # Handle Fabric API async response pattern
            if response.status_code == 202:
                # 202 Accepted - request is being processed asynchronously
                print("✅ Notebook creation accepted (async processing)")
                
                # Wait for processing and then find the notebook by name
                print("Waiting for notebook creation to complete...")
                time.sleep(10)  # Give Fabric time to process
                
                # Try to find the newly created notebook
                for attempt in range(5):  # Try 5 times with increasing delay
                    notebook_id = self.find_notebook_by_name(name)
                    if notebook_id:
                        print("✅ Created notebook: {}".format(name))
                        return notebook_id
                    
                    print("Waiting for notebook to appear (attempt {}/5)...".format(attempt + 1))
                    time.sleep(5 * (attempt + 1))  # Exponential backoff
                
                print("❌ Notebook creation timed out - notebook may still be processing")
                return None
                
            # Handle immediate response (status 200/201)
            try:
                data = response.json() if response.text.strip() else {}
                
                if response.status_code in [200, 201]:
                    notebook_id = data.get("id") if isinstance(data, dict) else None
                    if notebook_id:
                        print("✅ Created notebook: {}".format(name))
                        return notebook_id
                    else:
                        # Try to find by name as fallback
                        time.sleep(3)
                        notebook_id = self.find_notebook_by_name(name)
                        if notebook_id:
                            print("✅ Created notebook: {} (found by name)".format(name))
                            return notebook_id
                        else:
                            print("❌ No notebook ID found after creation")
                            return None
                else:
                    print("❌ Unexpected status code: {}".format(response.status_code))
                    return None
                    
            except ValueError as json_error:
                print("❌ Invalid JSON response: {}".format(json_error))
                print("Response text: {}".format(response.text[:500]))
                return None
            
        except requests.exceptions.RequestException as e:
            print("❌ Failed to create notebook {}: {}".format(name, e))
            if hasattr(e, 'response') and e.response is not None:
                print("Response status: {}".format(e.response.status_code))
                print("Response text: {}".format(e.response.text[:500]))
            return None
    
    def update_notebook(self, notebook_id: str, content: str) -> bool:
        """Update existing notebook content"""
        self.ensure_authentication()
        
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        # Encode content as base64
        content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        
        payload = {
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": content_b64,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            # Handle async response for updates too
            if response.status_code == 202:
                print("✅ Notebook update accepted (async processing)")
                time.sleep(5)  # Give time for processing
                return True
            elif response.status_code in [200, 201]:
                print("✅ Updated notebook: {}".format(notebook_id))
                return True
            else:
                print("❌ Unexpected update response: {}".format(response.status_code))
                return False
            
        except requests.exceptions.RequestException as e:
            print("❌ Failed to update notebook {}: {}".format(notebook_id, e))
            if hasattr(e, 'response') and e.response is not None:
                print("Response status: {}".format(e.response.status_code))
                print("Response text: {}".format(e.response.text[:500]))
            return False
    
    def get_notebook_content(self, notebook_id: str) -> Optional[str]:
        """Get notebook content for comparison"""
        self.ensure_authentication()
        
        url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/getDefinition"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            definition = data.get("definition", {})
            parts = definition.get("parts", [])
            
            for part in parts:
                if part.get("path") == "notebook-content.py":
                    payload = part.get("payload", "")
                    try:
                        decoded_content = base64.b64decode(payload).decode('utf-8')
                        return decoded_content
                    except Exception as decode_error:
                        print(f"Error decoding content: {decode_error}")
                        return None
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting notebook content: {e}")
            return None
    
    def has_notebook_changed(self, file_path: str, notebook_id: str) -> bool:
        """Check if local file content differs from Fabric notebook"""
        try:
            # Read local file content
            with open(file_path, 'r', encoding='utf-8') as f:
                local_content = f.read().strip()
            
            # Get Fabric notebook content
            fabric_content = self.get_notebook_content(notebook_id)
            
            if fabric_content is None:
                print("Could not retrieve Fabric content for comparison")
                return True  # If we can't compare, assume changed
            
            fabric_content = fabric_content.strip()
            
            # Basic content comparison
            if local_content == fabric_content:
                return False
            
            # Check if only minor differences (whitespace, comments)
            local_lines = [line.strip() for line in local_content.split('\n') if line.strip()]
            fabric_lines = [line.strip() for line in fabric_content.split('\n') if line.strip()]
            
            if local_lines == fabric_lines:
                print("Only whitespace differences detected")
                return False
            
            return True
            
        except Exception as e:
            print(f"Error comparing notebook content: {e}")
            return True  # If error, assume changed

    def deploy_notebook(self, file_path: str, notebook_name: str) -> bool:
        """Deploy a single notebook with robust async handling"""
        if not os.path.exists(file_path):
            print("File not found: {}".format(file_path))
            return False
        
        # Read file content once
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print("❌ Error reading file {}: {}".format(file_path, e))
            return False
        
        # Check if notebook exists
        notebook_id = self.find_notebook_by_name(notebook_name)
        
        if notebook_id:
            # Notebook exists - update it
            print("Updating existing notebook: {}".format(notebook_name))
            
            # Check if content has actually changed (optional - skip for reliability)
            try:
                if not self.has_notebook_changed(file_path, notebook_id):
                    print("Skipping {} - no changes detected".format(notebook_name))
                    return True
            except Exception as e:
                print("⚠️ Could not compare content, proceeding with update: {}".format(e))
            
            # Update existing notebook
            return self.update_notebook(notebook_id, content)
        else:
            # Notebook doesn't exist - create new one
            print("Creating new notebook: {}".format(notebook_name))
            
            # Create new notebook with async handling
            success = self.create_notebook_with_verification(notebook_name, content)
            return success
    
    def create_notebook_with_verification(self, name: str, content: str) -> bool:
        """Create notebook and verify it was actually created"""
        # Try to create the notebook
        result = self.create_notebook(name, content)
        
        # If create_notebook returns None (async case), verify by finding the notebook
        if result is None:
            print("Verifying notebook creation...")
            time.sleep(5)  # Additional wait time
            
            # Try to find the notebook to confirm it was created
            for attempt in range(3):
                notebook_id = self.find_notebook_by_name(name)
                if notebook_id:
                    print("✅ Verified notebook created: {}".format(name))
                    return True
                
                print("Verification attempt {}/3 - waiting...".format(attempt + 1))
                time.sleep(10)
            
            print("❌ Could not verify notebook creation: {}".format(name))
            return False
        
        # If create_notebook returned a valid ID, we're good
        return result is not None
    
    def deploy_all(self) -> bool:
        """Deploy all personal repos notebooks with async handling"""
        print("Starting Personal Repos Fabric notebook deployment...")
        
        success_count = 0
        total_count = len(self.notebook_mapping)
        
        for i, (file_path, notebook_name) in enumerate(self.notebook_mapping.items()):
            print("\n--- Deploying notebook {}/{} ---".format(i + 1, total_count))
            print("File: {}".format(file_path))
            print("Name: {}".format(notebook_name))
            
            try:
                if self.deploy_notebook(file_path, notebook_name):
                    success_count += 1
                    print("✅ Successfully deployed: {}".format(notebook_name))
                else:
                    print("❌ Failed to deploy: {}".format(notebook_name))
                
                # Longer wait between deployments for async processing
                if i < total_count - 1:  # Don't wait after last one
                    print("Waiting before next deployment...")
                    time.sleep(15)
                    
            except Exception as e:
                print("❌ Exception deploying {}: {}".format(notebook_name, str(e)))
        
        print("\n" + "="*50)
        print("Personal Repos Deployment Summary:")
        print("Successfully deployed: {}/{} notebooks".format(success_count, total_count))
        
        if success_count == total_count:
            print("✅ All personal repos notebooks deployed successfully!")
            return True
        elif success_count > 0:
            print("⚠️ Partial deployment success - {} notebooks deployed".format(success_count))
            print("Note: Some notebooks may still be processing asynchronously")
            return False
        else:
            print("❌ No notebooks were deployed successfully")
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
