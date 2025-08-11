"""
Test script for local deployment testing
Load credentials from .env file and test the deployment
"""

import os
import sys
from pathlib import Path

def load_env_file():
    """Load environment variables from .env file"""
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

    env_file = project_root / ".env"
    
    if not env_file.exists():
        print(".env file not found. Please create one with your Fabric credentials.")
        print("Example .env content:")
        print("FABRIC_TENANT_ID=your-tenant-id")
        print("FABRIC_CLIENT_ID=your-client-id") 
        print("FABRIC_CLIENT_SECRET=your-client-secret")
        print("FABRIC_WORKSPACE_ID=060223d7-8152-4dec-97d3-ef2d5d8c6644")
        return False
    
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value
    
    return True

def main():
    print("Testing Fabric notebook deployment locally...\n")
    
    # Load environment variables
    if not load_env_file():
        sys.exit(1)
    
    # Import and run deployer
    try:
        from deploy_notebooks import FabricNotebookDeployer
        
        deployer = FabricNotebookDeployer()
        success = deployer.deploy_all()
        
        if success:
            print("\n✅ Local test deployment successful!")
            sys.exit(0)
        else:
            print("\nLocal test deployment failed!")
            sys.exit(1)
            
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure you're running from the scripts directory")
        sys.exit(1)
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
