"""
Script to help create Azure Service Principal for Fabric API access
Run this locally to get the required credentials
"""

import subprocess
import json
import sys

def run_az_command(command):
    """Run Azure CLI command and return JSON result"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            return json.loads(result.stdout) if result.stdout.strip() else None
        else:
            print(f"Error: {result.stderr}")
            return None
    except Exception as e:
        print(f"Command failed: {e}")
        return None

def main():
    print("Creating Service Principal for Microsoft Fabric API access...\n")
    
    # Check if Azure CLI is installed
    print("1. Checking Azure CLI installation...")
    az_version = run_az_command("az --version")
    if not az_version:
        print("Azure CLI not found. Please install: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli")
        sys.exit(1)
    print("✅ Azure CLI found")
    
    # Login check
    print("\n2. Checking Azure login status...")
    account_info = run_az_command("az account show")
    if not account_info:
        print("Not logged in to Azure. Please run: az login")
        sys.exit(1)
    
    tenant_id = account_info.get("tenantId")
    print(f"✅ Logged in to tenant: {tenant_id}")
    
    # Create service principal
    print("\n3. Creating service principal...")
    sp_name = "stacktrend-fabric-cicd"
    
    create_command = f'az ad sp create-for-rbac --name "{sp_name}" --role contributor --scopes /subscriptions/{account_info.get("id")}'
    sp_result = run_az_command(create_command)
    
    if not sp_result:
        print("Failed to create service principal")
        sys.exit(1)
    
    print("✅ Service principal created successfully!")
    
    # Display credentials
    print("\n" + "="*60)
    print("GITHUB SECRETS TO ADD:")
    print("="*60)
    print(f"FABRIC_TENANT_ID = {tenant_id}")
    print(f"FABRIC_CLIENT_ID = {sp_result.get('appId')}")
    print(f"FABRIC_CLIENT_SECRET = {sp_result.get('password')}")
    print("FABRIC_WORKSPACE_ID = 060223d7-8152-4dec-97d3-ef2d5d8c6644")
    print("="*60)
    
    print("\nNext Steps:")
    print("1. Copy the above secrets to GitHub repository settings")
    print("2. Go to: https://github.com/sanchitvj/stacktrend/settings/secrets/actions")
    print("3. Add each secret using 'New repository secret'")
    print("4. Grant the service principal access to your Fabric workspace")
    
    print("\n⚠️ IMPORTANT: Save these credentials securely!")
    print("The client secret cannot be retrieved again after this.")

if __name__ == "__main__":
    main()
