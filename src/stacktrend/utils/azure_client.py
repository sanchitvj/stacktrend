"""Azure Storage client utilities."""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

from stacktrend.config.settings import settings


class AzureStorageClient:
    """Client for interacting with Azure Storage."""
    
    def __init__(self):
        """Initialize the Azure Storage client."""
        if not settings.AZURE_STORAGE_ACCOUNT_NAME or not settings.AZURE_STORAGE_ACCOUNT_KEY:
            raise ValueError("Azure Storage credentials not configured")
            
        account_url = f"https://{settings.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=settings.AZURE_STORAGE_ACCOUNT_KEY
        )
    
    def upload_json_data(
        self, 
        container_name: str, 
        blob_name: str, 
        data: Dict[Any, Any],
        overwrite: bool = True
    ) -> bool:
        """
        Upload JSON data to Azure Blob Storage.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob file
            data: Dictionary data to upload as JSON
            overwrite: Whether to overwrite existing blob
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_name
            )
            
            json_data = json.dumps(data, indent=2, default=str)
            blob_client.upload_blob(json_data, overwrite=overwrite)
            
            return True
            
        except AzureError as e:
            print(f"❌ Azure error uploading {blob_name}: {e}")
            return False
        except Exception as e:
            print(f"❌ Error uploading {blob_name}: {e}")
            return False
    
    def download_json_data(self, container_name: str, blob_name: str) -> Optional[Dict[Any, Any]]:
        """
        Download JSON data from Azure Blob Storage.
        
        Args:
            container_name: Name of the container
            blob_name: Name of the blob file
            
        Returns:
            Dict or None: Parsed JSON data or None if error
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            blob_data = blob_client.download_blob().readall()
            return json.loads(blob_data)
            
        except AzureError as e:
            print(f"❌ Azure error downloading {blob_name}: {e}")
            return None
        except Exception as e:
            print(f"❌ Error downloading {blob_name}: {e}")
            return None
    
    def generate_blob_name(self, prefix: str, suffix: str = "json") -> str:
        """
        Generate a timestamped blob name.
        
        Args:
            prefix: Prefix for the blob name
            suffix: File extension (default: json)
            
        Returns:
            str: Generated blob name with timestamp
        """
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H_%M_%S")
        return f"{prefix}/{timestamp}.{suffix}"
    
    def list_recent_bronze_files(self, hours_back: int = 24) -> List[str]:
        """
        List bronze files created within the specified time window.
        
        Args:
            hours_back: Number of hours to look back for files
            
        Returns:
            List of blob names for recent bronze files
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                settings.BRONZE_CONTAINER
            )
            
            # Calculate cutoff time
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
            
            recent_files = []
            blobs = container_client.list_blobs(name_starts_with="github_repositories/")
            
            for blob in blobs:
                # Only include actual JSON files (not directories)
                if blob.name.endswith('.json') and blob.last_modified:
                    # Check if blob was created recently
                    if blob.last_modified.replace(tzinfo=None) > cutoff_time:
                        recent_files.append(blob.name)
            
            # Sort by creation time (newest first)
            recent_files.sort(reverse=True)
            
            print(f"Found {len(recent_files)} recent bronze files: {recent_files[:3]}...")
            return recent_files
            
        except AzureError as e:
            print(f"❌ Azure error listing bronze files: {e}")
            return []
        except Exception as e:
            print(f"❌ Error listing bronze files: {e}")
            return [] 