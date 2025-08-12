"""
Microsoft Fabric Workspace Setup Script

This script automates the setup of Microsoft Fabric resources for the
Technology Adoption Observatory project, including:
- Lakehouse creation (Bronze, Silver, Gold)
- Notebook import and configuration
- Data Factory pipeline setup
- Initial workspace configuration

Usage:
    python -m stacktrend.setup.fabric_setup
"""

import sys
import json
import logging
from datetime import datetime
from pathlib import Path

from src.stacktrend.config.settings import Settings
from src.stacktrend.utils.fabric_client import FabricClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FabricWorkspaceSetup:
    """Handles the complete setup of Fabric workspace for the project."""
    
    def __init__(self):
        """Initialize setup with configuration validation."""
        self.settings = Settings()
        self.fabric_client = FabricClient(self.settings)
        self.setup_results = {
            "lakehouses": [],
            "notebooks": [],
            "pipelines": [],
            "datasets": [],
            "errors": []
        }
        
    def validate_prerequisites(self) -> bool:
        """Validate that all prerequisites are met before setup."""
        logger.info("Validating prerequisites...")
        
        try:
            # Validate configuration
            self.settings.validate()
            logger.debug("Configuration validation passed")
            
            # Test Fabric API connectivity
            workspaces = self.fabric_client._make_request("GET", "workspaces")  # noqa: F841
            logger.debug("Fabric API connectivity confirmed")
            
            # Check if target workspace exists
            workspace_id = self.settings.FABRIC_WORKSPACE_ID
            try:
                workspace = self.fabric_client._make_request("GET", f"workspaces/{workspace_id}")
                logger.info(f"Target workspace found: {workspace.json().get('displayName', 'Unknown')}")
            except Exception as e:
                logger.error(f"❌ Target workspace not accessible: {e}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Prerequisite validation failed: {e}")
            self.setup_results["errors"].append(str(e))
            return False
    
    def create_lakehouses(self) -> bool:
        """Create the medallion architecture lakehouses."""
        logger.info("Setting up medallion architecture lakehouses...")
        
        try:
            # Define lakehouse configurations
            lakehouses_config = [
                {
                    "name": self.settings.BRONZE_LAKEHOUSE_NAME,
                    "description": "Bronze layer - Raw GitHub API data",
                    "layer": "bronze"
                },
                {
                    "name": self.settings.SILVER_LAKEHOUSE_NAME, 
                    "description": "Silver layer - Cleaned and standardized data",
                    "layer": "silver"
                },
                {
                    "name": self.settings.GOLD_LAKEHOUSE_NAME,
                    "description": "Gold layer - Analytics-ready datasets",
                    "layer": "gold"
                }
            ]
            
            # Get existing lakehouses to avoid duplicates
            existing_lakehouses = {lh["displayName"]: lh for lh in self.fabric_client.list_lakehouses()}
            
            for config in lakehouses_config:
                name = config["name"]
                
                if name in existing_lakehouses:
                    logger.debug(f"Lakehouse already exists: {name}")
                    self.setup_results["lakehouses"].append({
                        "name": name,
                        "status": "existing",
                        "id": existing_lakehouses[name]["id"]
                    })
                else:
                    try:
                        lakehouse = self.fabric_client.create_lakehouse(name)
                        logger.info(f"Created lakehouse: {name}")
                        self.setup_results["lakehouses"].append({
                            "name": name,
                            "status": "created",
                            "id": lakehouse["id"]
                        })
                    except Exception as e:
                        logger.error(f"❌ Failed to create lakehouse {name}: {e}")
                        self.setup_results["errors"].append(f"Lakehouse creation failed: {name} - {e}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Lakehouse setup failed: {e}")
            self.setup_results["errors"].append(str(e))
            return False
    
    def create_folder_structure(self) -> bool:
        """Create the folder structure in lakehouses (conceptual - Fabric handles this)."""
        logger.info("Setting up lakehouse folder structure...")
        
        # Document expected folder structure
        folder_structure = {
            "bronze": [
                "github_repositories/",
                "github_contributors/", 
                "github_commits/",
                "api_responses/raw_json/"
            ],
            "silver": [
                "repositories/",
                "contributors/",
                "technologies/",
                "metrics/"
            ],
            "gold": [
                "technology_metrics/",
                "trend_analysis/",
                "predictions/",
                "dashboards/"
            ]
        }
        
        # Create folder structure documentation
        docs_path = Path("docs") / "lakehouse_structure.json"
        docs_path.parent.mkdir(exist_ok=True)
        
        with open(docs_path, 'w') as f:
            json.dump(folder_structure, f, indent=2)
        
        logger.info(f"Folder structure documented at: {docs_path}")
        logger.info("Folders will be created automatically when data is written")
        
        return True
    
    def import_notebooks(self) -> bool:
        """Import processing notebooks into Fabric workspace."""
        logger.info("Importing processing notebooks...")
        
        try:
            # Notebook configurations
            notebooks_config = [
                {
                    "name": "bronze_to_silver_transformation",
                    "source_file": "notebooks/bronze_to_silver_transformation.py",
                    "description": "Transform raw GitHub data from Bronze to Silver layer"
                },
                {
                    "name": "silver_to_gold_analytics", 
                    "source_file": "notebooks/silver_to_gold_analytics.py",
                    "description": "Generate analytics and metrics for Gold layer"
                },
                {
                    "name": "technology_taxonomy_mapping",
                    "source_file": "notebooks/technology_taxonomy.py", 
                    "description": "Technology categorization and mapping utilities"
                },
                {
                    "name": "data_quality_validation",
                    "source_file": "notebooks/data_quality.py",
                    "description": "Data quality checks and validation rules"
                }
            ]
            
            # Get existing notebooks
            existing_notebooks = {nb["displayName"]: nb for nb in self.fabric_client.list_notebooks()}
            
            for config in notebooks_config:
                name = config["name"]
                
                if name in existing_notebooks:
                    logger.debug(f"Notebook already exists: {name}")
                    self.setup_results["notebooks"].append({
                        "name": name,
                        "status": "existing",
                        "id": existing_notebooks[name]["id"]
                    })
                else:
                    try:
                        notebook = self.fabric_client.create_notebook(name)
                        logger.info(f"Created notebook: {name}")
                        logger.info(f"Manual step: Import content from {config['source_file']}")
                        self.setup_results["notebooks"].append({
                            "name": name,
                            "status": "created",
                            "id": notebook["id"],
                            "source_file": config["source_file"]
                        })
                    except Exception as e:
                        logger.error(f"❌ Failed to create notebook {name}: {e}")
                        self.setup_results["errors"].append(f"Notebook creation failed: {name} - {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Notebook import failed: {e}")
            self.setup_results["errors"].append(str(e))
            return False
    
    def create_data_pipelines(self) -> bool:
        """Create Data Factory pipelines for orchestration."""
        logger.info("Creating Data Factory pipelines...")
        
        try:
            # Pipeline configurations
            pipelines_config = [
                {
                    "name": self.settings.GITHUB_INGESTION_PIPELINE,
                    "description": "GitHub API data ingestion orchestration"
                },
                {
                    "name": self.settings.BRONZE_TO_SILVER_PIPELINE,
                    "description": "Bronze to Silver transformation pipeline"
                },
                {
                    "name": self.settings.SILVER_TO_GOLD_PIPELINE,
                    "description": "Silver to Gold analytics pipeline"
                }
            ]
            
            # Get existing pipelines
            existing_pipelines = {dp["displayName"]: dp for dp in self.fabric_client.list_data_pipelines()}
            
            for config in pipelines_config:
                name = config["name"]
                
                if name in existing_pipelines:
                    logger.debug(f"Pipeline already exists: {name}")
                    self.setup_results["pipelines"].append({
                        "name": name,
                        "status": "existing", 
                        "id": existing_pipelines[name]["id"]
                    })
                else:
                    try:
                        pipeline = self.fabric_client.create_data_pipeline(name)
                        logger.info(f"Created pipeline: {name}")
                        logger.info("Manual step: Configure pipeline activities in Fabric UI")
                        self.setup_results["pipelines"].append({
                            "name": name,
                            "status": "created",
                            "id": pipeline["id"]
                        })
                    except Exception as e:
                        logger.error(f"❌ Failed to create pipeline {name}: {e}")
                        self.setup_results["errors"].append(f"Pipeline creation failed: {name} - {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline creation failed: {e}")
            self.setup_results["errors"].append(str(e))
            return False
    
    def create_setup_documentation(self) -> None:
        """Create comprehensive setup documentation."""
        logger.info("Creating setup documentation...")
        
        docs_dir = Path("docs") / "fabric_setup"
        docs_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup results summary
        setup_summary = {
            "setup_date": datetime.now().isoformat(),
            "workspace_id": self.settings.FABRIC_WORKSPACE_ID,
            "workspace_name": self.settings.FABRIC_WORKSPACE_NAME,
            "results": self.setup_results
        }
        
        with open(docs_dir / "setup_results.json", 'w') as f:
            json.dump(setup_summary, f, indent=2)
        
        # Manual setup steps
        manual_steps = """# Manual Setup Steps for Microsoft Fabric

After running the automated setup, complete these manual steps:

## 1. Notebook Configuration
For each created notebook, you'll need to:
1. Open the notebook in Fabric
2. Copy the content from the corresponding Python file
3. Attach to a Spark compute pool
4. Test run the notebook

## 2. Pipeline Configuration  
For each created pipeline:
1. Open the pipeline in Data Factory
2. Add activities (Copy Data, Notebook, etc.)
3. Configure triggers and schedules
4. Set up error handling and notifications

## 3. Power BI Setup
1. Create a new Power BI report in the workspace
2. Connect to the Gold lakehouse as data source
3. Build dashboards using the technology metrics

## 4. Security Configuration
1. Set up workspace permissions
2. Configure service principal access
3. Set up data access policies

## 5. Monitoring Setup
1. Configure pipeline monitoring
2. Set up cost alerts
3. Enable activity logging

## Resources Created:
"""

        for category, items in self.setup_results.items():
            if items and category != "errors":
                manual_steps += f"\n### {category.title()}:\n"
                for item in items:
                    if isinstance(item, dict):
                        manual_steps += f"- {item.get('name', 'Unknown')} (Status: {item.get('status', 'Unknown')})\n"

        if self.setup_results["errors"]:
            manual_steps += "\n### Errors Encountered:\n"
            for error in self.setup_results["errors"]:
                manual_steps += f"- {error}\n"

        with open(docs_dir / "manual_steps.md", 'w') as f:
            f.write(manual_steps)
        
        logger.info(f"Documentation created at: {docs_dir}")
    
    def run_setup(self) -> bool:
        """Run the complete Fabric workspace setup."""
        logger.info("Starting Microsoft Fabric workspace setup...")
        logger.info("=" * 60)
        
        # Validate prerequisites
        if not self.validate_prerequisites():
            logger.error("❌ Prerequisites validation failed. Setup aborted.")
            return False
        
        success = True
        
        # Run setup steps
        steps = [
            ("Create Lakehouses", self.create_lakehouses),
            ("Setup Folder Structure", self.create_folder_structure),
            ("Import Notebooks", self.import_notebooks),
            ("Create Pipelines", self.create_data_pipelines)
        ]
        
        for step_name, step_function in steps:
            logger.info(f"\n{'='*20} {step_name} {'='*20}")
            if not step_function():
                logger.error(f"❌ {step_name} failed")
                success = False
            else:
                logger.info(f"{step_name} completed")
        
        # Create documentation regardless of success/failure
        self.create_setup_documentation()
        
        # Print summary
        logger.info("\n" + "="*60)
        if success and not self.setup_results["errors"]:
            logger.info("FABRIC WORKSPACE SETUP COMPLETED SUCCESSFULLY")
            logger.info("Check the docs/fabric_setup/ folder for next steps")
        else:
            logger.warning("SETUP COMPLETED WITH ISSUES")
            logger.warning("Check setup_results.json for details")
            logger.warning("Review manual_steps.md for required actions")
        
        logger.info("="*60)
        return success


def main():
    """Main entry point for Fabric setup."""
    try:
        setup = FabricWorkspaceSetup()
        success = setup.run_setup()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Setup failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()