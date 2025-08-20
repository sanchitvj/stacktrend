# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_silver_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
# MAGIC %md
# MAGIC # Personal Repository Bronze to Silver Transformation
# MAGIC 
# MAGIC Microsoft Fabric Notebook for processing personal GitHub repository data with LLM classification
# MAGIC 
# MAGIC This notebook transforms raw personal repository data into clean, classified data with metrics.

# COMMAND ----------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime, timedelta
import os
import sys

# Initialize Spark Session
spark = SparkSession.builder.appName("Personal_Repos_Bronze_to_Silver").getOrCreate()

# COMMAND ----------
# Mount additional lakehouses for cross-lakehouse access using secure context
try:
    from notebookutils import mssparkutils
    
    # Get current workspace context securely
    workspace_id = mssparkutils.env.getWorkspaceId()
    
    # Mount Bronze lakehouse using lakehouse name (Fabric resolves the ID securely)
    bronze_mount = "/mnt/bronze"
    bronze_abfs = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/stacktrend_bronze_lh.Lakehouse"
    
    # Check if already mounted
    existing_mounts = [mount.mountPoint for mount in mssparkutils.fs.mounts()]
    if bronze_mount not in existing_mounts:
        mssparkutils.fs.mount(bronze_abfs, bronze_mount)
        print(f"Mounted Bronze lakehouse at {bronze_mount}")
    else:
        print(f"Bronze lakehouse already mounted at {bronze_mount}")
        
except Exception as e:
    print(f"WARNING: Mount failed, will use cross-lakehouse table references: {e}")

# COMMAND ----------
# PARAMETERS CELL: Define parameters that Data Factory will pass
# This cell must be marked as "parameter cell" in Fabric (click ... → Toggle parameter cell)
# Only set if not already defined (prevents overwriting actual values from Data Factory)
if 'azure_openai_api_key' not in locals() or not azure_openai_api_key:  # noqa: F821
    azure_openai_api_key = ""
if 'azure_openai_endpoint' not in locals() or not azure_openai_endpoint:  # noqa: F821
    azure_openai_endpoint = ""
if 'azure_openai_api_version' not in locals() or not azure_openai_api_version:  # noqa: F821
    azure_openai_api_version = "2025-01-01-preview"
if 'azure_openai_model' not in locals() or not azure_openai_model:  # noqa: F821
    azure_openai_model = "o4-mini"

# COMMAND ----------
# SECURE: Configure Azure OpenAI credentials from Data Factory parameters

print("Configuring Azure OpenAI credentials from Data Factory parameters...")

# Validate that required parameters were passed from Data Factory
if not azure_openai_api_key or azure_openai_api_key == "":
    raise ValueError("Azure OpenAI API key is required for LLM classification")

if not azure_openai_endpoint or azure_openai_endpoint == "":
    raise ValueError("Azure OpenAI endpoint is required for LLM classification")

print("✅ Azure OpenAI credentials configured")
print(f"   Endpoint: {azure_openai_endpoint}")
print(f"   Model: {azure_openai_model}")
print(f"   API Version: {azure_openai_api_version}")

# Set environment variables for the LLM classifier (exactly like original)
os.environ['AZURE_OPENAI_API_KEY'] = azure_openai_api_key
os.environ['AZURE_OPENAI_ENDPOINT'] = azure_openai_endpoint
os.environ['AZURE_OPENAI_API_VERSION'] = azure_openai_api_version
os.environ['AZURE_OPENAI_MODEL'] = azure_openai_model

# COMMAND ----------
# Configuration
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
LOOKBACK_DAYS = 30  # For velocity calculations

# COMMAND ----------
# MAGIC %md
# MAGIC ## LLM-Based Repository Classification System

# COMMAND ----------
# EXACT copy of original working notebook's LLM setup
def ensure_stacktrend_imports():
    """
    Install and import stacktrend package strictly inside a function to satisfy linters.
    Note: Dynamic imports are necessary here as packages don't exist until installed.
    """
    import os
    
    # First try to import - maybe it's already installed
    try:
        from stacktrend.utils.llm_classifier import (
            LLMRepositoryClassifier as _LLMRepositoryClassifier,
            create_repository_data_from_dict as _create_repository_data_from_dict,
        )
        from stacktrend.config.settings import settings as _settings
        print("Stacktrend package already available")
        return _LLMRepositoryClassifier, _create_repository_data_from_dict, _settings
    except ImportError:
        print("Stacktrend package not found, installing...")
    
    try:
        # Try different installation approaches
        github_read_token = os.environ.get("GITHUB_READ_TOKEN")
        repo_ref = os.environ.get("STACKTREND_GIT_REF", "dev")
        
        install_attempts = []
        
        # Attempt 1: Direct ZIP download (avoids git operations completely)
        install_attempts.append(f"https://github.com/sanchitvj/stacktrend/archive/{repo_ref}.zip")
        install_attempts.append("https://github.com/sanchitvj/stacktrend/archive/main.zip")
        
        # Attempt 2: Use pip programmatically instead of subprocess
        try:
            import pip
            print("Trying pip programmatic installation...")
            for url in install_attempts[:2]:  # Only try ZIP downloads first
                try:
                    print(f"Installing from {url}")
                    # Use pip's internal API - different approaches for different pip versions
                    try:
                        # Modern pip
                        from pip._internal import main as pip_main
                        pip_main(['install', '--upgrade', '--no-cache-dir', url])
                    except ImportError:
                        # Older pip
                        pip.main(['install', '--upgrade', '--no-cache-dir', url])
                    print("Pip programmatic installation succeeded")
                    break
                except Exception as pip_error:
                    print(f"Pip programmatic install failed: {pip_error}")
                    continue
        except ImportError:
            print("Pip module not available for programmatic use")
            
        # Attempt 2.5: Try using importlib and direct download
        try:
            import urllib.request
            import tempfile
            import zipfile
            print("Trying direct download and install...")
            
            for url in install_attempts[:2]:  # ZIP URLs only
                try:
                    print(f"Downloading {url}")
                    with tempfile.TemporaryDirectory() as temp_dir:
                        zip_path = f"{temp_dir}/stacktrend.zip"
                        urllib.request.urlretrieve(url, zip_path)
                        
                        # Extract zip
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            zip_ref.extractall(temp_dir)
                        
                        # Find the extracted directory
                        extracted_dirs = [d for d in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, d))]
                        if extracted_dirs:
                            extracted_path = os.path.join(temp_dir, extracted_dirs[0])
                            
                            # Add to Python path
                            if extracted_path not in sys.path:
                                sys.path.insert(0, extracted_path)
                            
                            print("Direct download installation succeeded")
                            break
                except Exception as download_error:
                    print(f"Direct download failed: {download_error}")
                    continue
        except Exception as e:
            print(f"Direct download approach failed: {e}")
        
        # Attempt 3: Git methods as last resort
        if github_read_token:
            install_attempts.append(f"git+https://{github_read_token}@github.com/sanchitvj/stacktrend.git@{repo_ref}")
        install_attempts.append(f"git+https://github.com/sanchitvj/stacktrend.git@{repo_ref}")
        
        # Attempt 4: Main branch git fallback
        if github_read_token:
            install_attempts.append(f"git+https://{github_read_token}@github.com/sanchitvj/stacktrend.git@main")
        install_attempts.append("git+https://github.com/sanchitvj/stacktrend.git@main")
        
        # First, upgrade critical dependencies with force reload
        print("Upgrading critical dependencies with force reload...")
        try:
            import sys
            import subprocess
            
            # Force uninstall and reinstall to avoid cached imports
            subprocess.run([
                sys.executable, "-m", "pip", "uninstall", "-y", 
                "typing_extensions", "pydantic", "openai", "typing-inspection"
            ], timeout=60, capture_output=True, text=True)
            
            print("Uninstalled old versions, installing new ones...")
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", "--no-cache-dir",
                "typing_extensions>=4.12.0", "pydantic>=2.8.0", "openai>=1.35.0", "nest_asyncio"
            ], timeout=120, capture_output=True, text=True)
            
            if result.returncode == 0:
                print("Dependencies upgraded successfully")
            else:
                print(f"Dependency upgrade failed: {result.stderr}")
                
        except Exception as dep_error:
            print(f"Dependency upgrade error: {dep_error}")
        
        # Now try each installation method
        for i, install_url in enumerate(install_attempts, 1):
            print(f"Installation attempt {i}: {install_url}")
            
            try:
                import sys
                import subprocess
                
                # Choose installation method based on URL type
                if install_url.startswith('http') and install_url.endswith('.zip'):
                    # ZIP download method
                    cmd = [
                        sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", 
                        "--no-cache-dir", install_url
                    ]
                else:
                    # Git method
                    cmd = [
                        sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", 
                        "--no-cache-dir", install_url
                    ]
                
                print(f"Running command: {' '.join(cmd)}")
                
                # Redirect stderr to devnull for git operations
                with open(os.devnull, 'w') as devnull:
                    result = subprocess.run(cmd, timeout=300, stdout=subprocess.PIPE, 
                                          stderr=devnull if 'git+' in install_url else subprocess.PIPE, 
                                          text=True)
                
                if result.returncode == 0:
                    print(f"Installation successful from: {install_url}")
                    break
                else:
                    print(f"Installation failed with exit code {result.returncode}")
                    if result.stderr and 'git+' not in install_url:
                        print(f"Error details: {result.stderr[:200]}")
                    continue
                    
            except subprocess.TimeoutExpired:
                print(f"Installation timeout for attempt {i}")
                continue
            except Exception as install_error:
                print(f"Installation error for attempt {i}: {install_error}")
                continue
        else:
            # All installation attempts failed, try force dependency reset
            print("All installation attempts failed, trying complete dependency reset...")
            try:
                import sys
                import subprocess
                
                # Uninstall everything first
                subprocess.run([
                    sys.executable, "-m", "pip", "uninstall", "-y", 
                    "typing_extensions", "pydantic", "openai", "typing-inspection", "stacktrend"
                ], timeout=60, capture_output=True, text=True)
                
                # Clear all related modules from memory
                modules_to_clear = []
                for module_name in list(sys.modules.keys()):
                    if any(mod in module_name for mod in ['stacktrend', 'azure', 'openai', 'pydantic']):
                        modules_to_clear.append(module_name)
                
                for module_name in modules_to_clear:
                    try:
                        del sys.modules[module_name]
                    except KeyError:
                        pass
                
                # Install with specific compatible versions
                result = subprocess.run([
                    sys.executable, "-m", "pip", "install", "--no-cache-dir",
                    "typing_extensions==4.12.2", "pydantic==2.8.2", "openai==1.35.15", "nest_asyncio"
                ], timeout=120, capture_output=True, text=True)
                
                if result.returncode != 0:
                    print(f"Dependency reset failed: {result.stderr}")
                    raise Exception("Complete dependency reset failed")
                
                print("Dependencies reset successful, trying git install...")
                result = subprocess.run([
                    sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall",
                    "git+https://github.com/sanchitvj/stacktrend.git"
                ], timeout=180, capture_output=True, text=True)
                
                if result.returncode != 0:
                    raise Exception(f"Final git install failed: {result.stderr}")
                    
            except Exception as reset_error:
                print(f"Complete dependency reset failed: {reset_error}")
                raise Exception(f"Cannot install stacktrend package after all attempts: {reset_error}")
        
        # Clear any cached imports to ensure fresh import
        modules_to_clear = [k for k in sys.modules.keys() if k.startswith('stacktrend')]
        for module in modules_to_clear:
            del sys.modules[module]
        
        # Import after installation
        from stacktrend.utils.llm_classifier import (
            LLMRepositoryClassifier as _LLMRepositoryClassifier,
            create_repository_data_from_dict as _create_repository_data_from_dict,
        )
        from stacktrend.config.settings import settings as _settings
        
        print("✅ Successfully imported stacktrend modules")
        return _LLMRepositoryClassifier, _create_repository_data_from_dict, _settings
        
    except Exception as e:
        print(f"ERROR: CRITICAL: Failed to install stacktrend package: {e}")
        print("LLM classification is MANDATORY and cannot proceed without it")
        raise Exception(f"LLM classification setup failed: {e}")

def classify_repositories_with_llm(repositories_df):
    """
    Smart LLM classification - only classify repos that need it (not already well-classified)
    """
    LLMRepositoryClassifier, create_repository_data_from_dict, settings = ensure_stacktrend_imports()
    
    try:
        # Check existing Silver data to avoid re-classifying well-classified repos
        repos_needing_llm = repositories_df
        repos_for_metrics_only = spark.createDataFrame([], repositories_df.schema)
        
        try:
            existing_silver_df = spark.table("github_my_portfolio")
            
            # Get repos that are already well-classified (confidence >= 0.8 and not Other/unknown)
            well_classified = (existing_silver_df
                .filter(
                    (F.col("technology_category") != "Other") & 
                    (F.col("technology_subcategory") != "unknown") &
                    (F.col("classification_confidence") >= 0.8)
                )
                .select("repository_id", "technology_category", "technology_subcategory", "classification_confidence")
            )
            
            # Split repos: those needing LLM vs those needing metrics-only update  
            repos_needing_llm = repositories_df.join(well_classified, "repository_id", "left_anti")
            repos_for_metrics_only = repositories_df.join(well_classified, "repository_id", "inner")
            
            llm_count = repos_needing_llm.count()
            metrics_count = repos_for_metrics_only.count()
            
            print("Smart Classification Strategy:")
            print(f"  Repos needing LLM classification: {llm_count}")
            print(f"  Repos with metrics-only update: {metrics_count}")
            print(f"  Cost savings: ~${(metrics_count * 0.00006):.3f} (skipped {metrics_count} LLM calls)")
            
        except Exception as e:
            print(f"No existing Silver data found: {e}")
            print("Will classify all repositories with LLM")
        
        # Only run LLM on repos that need it
        if repos_needing_llm.count() == 0:
            print("No repositories need LLM classification")
            return repositories_df.withColumn("technology_category", F.lit("Other")).withColumn("technology_subcategory", F.lit("unknown")).withColumn("classification_confidence", F.lit(0.1))
        
        # Convert Spark DataFrame to list of repository data (only for repos needing LLM)
        repo_data_list = []
        repos_collected = repos_needing_llm.collect()
        
        for row in repos_collected:
            # Convert PySpark Row to dict for safe access
            row_dict = row.asDict()
            
            repo_data = create_repository_data_from_dict({
                'repository_id': row_dict['repository_id'],
                'name': row_dict['name'],
                'full_name': row_dict['full_name'],
                'description': row_dict.get('description'),
                'topics': row_dict.get('topics', []),
                'language': row_dict.get('language'),
                'stargazers_count': row_dict.get('stargazers_count', 0)
            })
            repo_data_list.append(repo_data)
        
        print(f"Running LLM classification on {repos_needing_llm.count()} repositories...")
        
        # Initialize LLM classifier using environment variables directly (EXACT same as original)
        try:
            classifier = LLMRepositoryClassifier(
                api_key=os.environ.get('AZURE_OPENAI_API_KEY'),
                endpoint=os.environ.get('AZURE_OPENAI_ENDPOINT'),
                api_version=os.environ.get('AZURE_OPENAI_API_VERSION', '2025-01-01-preview'),
                model=os.environ.get('AZURE_OPENAI_MODEL', 'o4-mini')
            )
        except Exception as e:
            raise Exception(f"LLM Classifier initialization failed: {e}")
        
        # Classify repositories
        try:
            classifications = classifier.classify_repositories_sync(repo_data_list)
        except Exception as e:
            raise Exception(f"LLM classification failed: {e}")
        
        # Convert results back to Spark DataFrame format
        classification_map = {}
        for c in classifications:
            classification_map[c.repo_id] = {
                'primary_category': c.primary_category,
                'subcategory': c.subcategory,
                'confidence': c.confidence
            }
        
        # Add classifications to original DataFrame
        def add_classification(repo_id):
            str_repo_id = str(repo_id)
            classification = classification_map.get(str_repo_id, {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return classification['primary_category']

        def add_subcategory(repo_id):
            str_repo_id = str(repo_id)
            classification = classification_map.get(str_repo_id, {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return classification['subcategory']

        def add_confidence(repo_id):
            str_repo_id = str(repo_id)
            classification = classification_map.get(str_repo_id, {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return float(classification['confidence'])
        
        add_classification_udf = F.udf(add_classification, StringType())
        add_subcategory_udf = F.udf(add_subcategory, StringType())
        add_confidence_udf = F.udf(add_confidence, DoubleType())
        
        # Add classifications to repos that needed LLM
        classified_repos = (repos_needing_llm
                .withColumn("technology_category", add_classification_udf(F.col("repository_id")))
                .withColumn("technology_subcategory", add_subcategory_udf(F.col("repository_id")))
                .withColumn("classification_confidence", add_confidence_udf(F.col("repository_id"))))
        
        # For repos with metrics-only update, preserve existing classifications
        if repos_for_metrics_only.count() > 0:
            # Create lookup map for existing classifications
            existing_classifications = {row.repository_id: (row.technology_category, row.technology_subcategory, row.classification_confidence) 
                                     for row in well_classified.collect()}
            
            def get_existing_category(repo_id):
                return existing_classifications.get(repo_id, ("Other", "unknown", 0.1))[0]
            
            def get_existing_subcategory(repo_id):
                return existing_classifications.get(repo_id, ("Other", "unknown", 0.1))[1]
                
            def get_existing_confidence(repo_id):
                return existing_classifications.get(repo_id, ("Other", "unknown", 0.1))[2]
            
            # Create UDFs for existing classifications  
            existing_category_udf = F.udf(get_existing_category, StringType())
            existing_subcategory_udf = F.udf(get_existing_subcategory, StringType()) 
            existing_confidence_udf = F.udf(get_existing_confidence, DoubleType())
            
            # Add existing classifications using same pattern as LLM classifications
            metrics_with_classification = (repos_for_metrics_only
                .withColumn("technology_category", existing_category_udf(F.col("repository_id")))
                .withColumn("technology_subcategory", existing_subcategory_udf(F.col("repository_id")))
                .withColumn("classification_confidence", existing_confidence_udf(F.col("repository_id"))))
            
            # Union both DataFrames
            final_classified_df = classified_repos.union(metrics_with_classification)
        else:
            final_classified_df = classified_repos
        
        print("✅ Repository classification completed")
        return final_classified_df
        
    except Exception as e:
        print(f"❌ Repository classification failed: {e}")
        raise Exception(f"LLM classification is mandatory for Silver layer: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Loading from Bronze Layer

# COMMAND ----------
# Read Personal repositories data from Bronze layer
try:
    # Try mounted path first, fallback to cross-lakehouse reference
    # Use cross-lakehouse table reference directly (same as original working notebooks)
    repos_bronze_df = spark.table("stacktrend_bronze_lh.github_my_repos")
    print("SUCCESS: Successfully loaded personal repos using cross-lakehouse reference")
    
    # Show basic statistics
    total_repos = repos_bronze_df.count()
    print("Bronze Personal Repos Data: {} repositories".format(total_repos))
    
except Exception as e:
    print(f"❌ Error loading repository data: {e}")
    raise Exception(f"Cannot proceed without Bronze repository data: {e}")

# Read Activity data from Bronze layer
try:
    # Use cross-lakehouse table reference directly (same as original working notebooks)
    activity_bronze_df = spark.table("stacktrend_bronze_lh.github_repo_activity")
    print("SUCCESS: Successfully loaded activity using cross-lakehouse reference")
    
    total_activities = activity_bronze_df.count()
    print("Bronze Activity Data: {} activities".format(total_activities))
    
except Exception as e:
    print(f"❌ Error loading activity data: {e}")
    raise Exception(f"Cannot proceed without Bronze activity data: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Repository Data Cleaning and Enrichment

# COMMAND ----------
if repos_bronze_df is not None:
    print("Processing personal repository data...")
    
    # Clean and standardize repository data
    repos_clean_df = repos_bronze_df.select(
        F.col("repository_id"),
        F.col("name"),
        F.col("full_name"),
        F.col("description"),
        F.col("private"),
        F.col("language").alias("primary_language"),
        F.col("stargazers_count"),
        F.col("forks_count"),
        F.col("open_issues_count"),
        F.col("size"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("pushed_at"),
        F.col("topics"),
        F.col("license_name"),
        F.col("default_branch"),
        F.col("partition_date")
    ).filter(
        F.col("repository_id").isNotNull()
    )
    
    # Clean description
    repos_clean_df = repos_clean_df.withColumn(
        "description_clean",
        F.when(F.col("description").isNotNull(), 
               F.trim(F.regexp_replace(F.col("description"), "[\\r\\n\\t]+", " ")))
         .otherwise("No description provided")
    )
    
    # Calculate derived metrics
    repos_clean_df = repos_clean_df.withColumn(
        "repository_size_mb", (F.col("size") / 1024.0).cast(DoubleType())
    ).withColumn(
        "days_since_creation", 
        F.datediff(F.current_date(), F.col("created_at").cast("date"))
    ).withColumn(
        "days_since_last_push",
        F.datediff(F.current_date(), F.col("pushed_at").cast("date"))
    ).withColumn(
        "is_active",
        F.when(F.col("days_since_last_push") <= 30, True).otherwise(False)
    ).withColumn(
        "visibility",
        F.when(F.col("private"), "private").otherwise("public")
    )
    
    # Calculate quality score based on multiple factors
    repos_clean_df = repos_clean_df.withColumn(
        "quality_score",
        F.least(F.lit(1.0),
            (F.when(F.col("description").isNotNull() & (F.length(F.col("description")) > 10), 0.2).otherwise(0.0) +
             F.when(F.col("license_name").isNotNull(), 0.2).otherwise(0.0) +
             F.when(F.size(F.col("topics")) > 0, 0.2).otherwise(0.0) +
             F.when(F.col("stargazers_count") > 0, 0.2).otherwise(0.0) +
             F.when(F.col("is_active"), 0.2).otherwise(0.0))
        ).cast(DoubleType())
    )
    
    # Determine popularity tier
    repos_clean_df = repos_clean_df.withColumn(
        "popularity_tier",
        F.when(F.col("stargazers_count") >= 100, "high")
         .when(F.col("stargazers_count") >= 10, "medium")
         .otherwise("low")
    )
    
    # Extract language distribution (simplified)
    repos_clean_df = repos_clean_df.withColumn(
        "language_distribution",
        F.when(F.col("primary_language").isNotNull(), 
               F.create_map(F.col("primary_language"), F.lit(1.0)))
         .otherwise(F.create_map())
    )
    
    # Apply LLM classification
    print("Applying LLM-based technology classification...")
    repos_classified_df = classify_repositories_with_llm(repos_clean_df)
    
    # Add processing metadata
    repos_final_df = repos_classified_df.withColumn(
        "processed_timestamp", F.lit(datetime.now())
    )
    
    print(f"✅ Personal repositories processed: {repos_final_df.count()} records")
    
else:
    print("No repository data available for processing")
    repos_final_df = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Activity Metrics Calculation

# COMMAND ----------
if activity_bronze_df is not None and repos_final_df is not None:
    print("Calculating activity metrics...")
    
    # Calculate activity metrics for different periods
    activity_periods = ["7d", "30d", "90d"]
    all_activity_metrics = []
    
    for period in activity_periods:
        days = int(period[:-1])
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Filter activity for this period
        period_activity = activity_bronze_df.filter(
            F.col("activity_date") >= F.lit(cutoff_date)
        )
        
        # Calculate metrics per repository
        repo_activity_metrics = period_activity.groupBy("repository_id").agg(
            F.sum(F.when(F.col("activity_type") == "commit", 1).otherwise(0)).alias("total_commits"),
            F.sum(F.when(F.col("activity_type") == "issue", 1).otherwise(0)).alias("total_issues"),
            F.sum(F.when(F.col("activity_type") == "release", 1).otherwise(0)).alias("total_releases"),
            F.sum(F.coalesce(F.col("additions"), F.lit(0))).alias("lines_added"),
            F.sum(F.coalesce(F.col("deletions"), F.lit(0))).alias("lines_deleted"),
            F.sum(F.coalesce(F.col("changed_files"), F.lit(0))).alias("files_changed"),
            F.max("activity_date").alias("last_activity_date")
        ).withColumn(
            "measurement_period", F.lit(period)
        ).withColumn(
            "commit_frequency", (F.col("total_commits") / F.lit(days)).cast(DoubleType())
        ).withColumn(
            "issue_resolution_rate", 
            F.when(F.col("total_issues") > 0, 
                   F.col("total_issues") / F.greatest(F.lit(1), F.col("total_issues"))).otherwise(0.0)
        ).withColumn(
            "development_velocity",
            F.least(F.lit(1.0),
                (F.col("commit_frequency") * 0.4 +
                 F.least(F.lit(1.0), F.col("lines_added") / 1000.0) * 0.3 +
                 F.least(F.lit(1.0), F.col("files_changed") / 100.0) * 0.3)).cast(DoubleType())
        ).withColumn(
            "activity_trend",
            F.when(F.col("development_velocity") >= 0.7, "increasing")
             .when(F.col("development_velocity") >= 0.3, "stable")
             .otherwise("decreasing")
        ).withColumn(
            "processed_timestamp", F.lit(datetime.now())
        ).withColumn(
            "partition_date", F.lit(PROCESSING_DATE)
        )
        
        all_activity_metrics.append(repo_activity_metrics)
    
    # Union all periods
    if all_activity_metrics:
        activity_metrics_df = all_activity_metrics[0]
        for df in all_activity_metrics[1:]:
            activity_metrics_df = activity_metrics_df.union(df)
        
        print(f"✅ Activity metrics calculated: {activity_metrics_df.count()} records")
    else:
        activity_metrics_df = None
        
else:
    print("No activity data available for metrics calculation")
    activity_metrics_df = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Personal Portfolio to Silver Layer

# COMMAND ----------
if repos_final_df is not None:
    print("Saving personal portfolio to Silver layer...")
    
    # Select final columns for Silver layer
    portfolio_silver_df = repos_final_df.select(
        "repository_id",
        "name",
        "full_name",
        "description_clean",
        "technology_category",
        "technology_subcategory",
        "classification_confidence",
        "primary_language",
        "language_distribution",
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "repository_size_mb",
        "days_since_creation",
        "days_since_last_push",
        "is_active",
        "visibility",
        "quality_score",
        "popularity_tier",
        "processed_timestamp",
        "partition_date"
    )
    
    # Smart Delta merge to Silver lakehouse using SQL MERGE pattern
    try:
        # Check if Silver table exists
        table_exists = True
        try:
            existing_silver = spark.table("github_my_portfolio")
            existing_silver_count = existing_silver.count()
            print(f"Found existing Silver portfolio table with {existing_silver_count} records")
        except Exception as e:
            table_exists = False
            print("Silver portfolio table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        clean_records = portfolio_silver_df.count()
        
        if table_exists and clean_records > 0:
            print("Performing Personal Portfolio Silver layer Delta merge...")
            
            portfolio_silver_df.createOrReplaceTempView("new_portfolio_silver_data")
            
            # SMART MERGE: Preserve good classifications, update metrics
            merge_sql = """
            MERGE INTO github_my_portfolio AS target
            USING new_portfolio_silver_data AS source
            ON target.repository_id = source.repository_id
            
            WHEN MATCHED THEN
              UPDATE SET
                name = source.name,
                full_name = source.full_name,
                description_clean = source.description_clean,
                primary_language = source.primary_language,
                language_distribution = source.language_distribution,
                stargazers_count = source.stargazers_count,
                forks_count = source.forks_count,
                open_issues_count = source.open_issues_count,
                repository_size_mb = source.repository_size_mb,
                days_since_creation = source.days_since_creation,
                days_since_last_push = source.days_since_last_push,
                is_active = source.is_active,
                visibility = source.visibility,
                quality_score = source.quality_score,
                popularity_tier = source.popularity_tier,
                processed_timestamp = source.processed_timestamp,
                partition_date = source.partition_date,
                
                -- Only update technology fields if current classification is poor
                technology_category = CASE 
                    WHEN target.technology_category = 'Other' OR target.technology_category IS NULL 
                    THEN source.technology_category 
                    ELSE target.technology_category 
                END,
                technology_subcategory = CASE 
                    WHEN target.technology_subcategory = 'unknown' OR target.technology_subcategory IS NULL 
                    THEN source.technology_subcategory 
                    ELSE target.technology_subcategory 
                END,
                classification_confidence = CASE 
                    WHEN target.technology_category = 'Other' OR target.technology_subcategory = 'unknown' 
                    THEN source.classification_confidence 
                    ELSE target.classification_confidence 
                END
                
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            
            # Get merge statistics
            final_silver_count = spark.table("github_my_portfolio").count()
            new_records = final_silver_count - existing_silver_count
            updated_records = clean_records - max(0, new_records)
            
            print("Personal Portfolio Silver layer Delta merge completed:")
            print(f"  Total records now: {final_silver_count}")
            print(f"  New records added: {max(0, new_records)}")
            print(f"  Records updated: {updated_records}")
            
        else:
            # First time or no data - use overwrite
            if clean_records > 0:
                (portfolio_silver_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date", "technology_category")
                 .saveAsTable("github_my_portfolio"))
                print(f"✅ Created new Silver portfolio table with {clean_records} records")
            else:
                print("No clean portfolio records to save")
        
    except Exception as e:
        print(f"❌ Error with Portfolio Silver Delta merge: {e}")
        print("Falling back to overwrite mode...")
        try:
            (portfolio_silver_df
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .partitionBy("partition_date", "technology_category")
             .saveAsTable("github_my_portfolio"))
            print(f"✅ Fallback successful: Saved {clean_records} records")
        except Exception as fallback_error:
            print(f"❌ Fallback also failed: {fallback_error}")
            raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Activity Metrics to Silver Layer

# COMMAND ----------
if activity_metrics_df is not None:
    print("Saving activity metrics to Silver layer...")
    
    # Smart Delta merge to Silver lakehouse using SQL MERGE pattern
    try:
        # Check if table exists
        table_exists = True
        try:
            existing_activity = spark.table("github_activity_metrics")
            existing_count = existing_activity.count()
            print(f"Found existing activity metrics table with {existing_count} records")
        except Exception as e:
            table_exists = False
            print("Activity metrics table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        metrics_records = activity_metrics_df.count()
        
        if table_exists and metrics_records > 0:
            print("Performing Activity Metrics Delta merge...")
            
            activity_metrics_df.createOrReplaceTempView("new_activity_metrics_data")
            
            # SQL MERGE: Update activity metrics
            merge_sql = """
            MERGE INTO github_activity_metrics AS target
            USING new_activity_metrics_data AS source
            ON target.repository_id = source.repository_id AND target.measurement_period = source.measurement_period
            
            WHEN MATCHED THEN
              UPDATE SET *
              
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            print("✅ Activity metrics merged successfully")
            
        else:
            # First time - create new table
            if metrics_records > 0:
                (activity_metrics_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date", "measurement_period")
                 .saveAsTable("github_activity_metrics"))
                print(f"✅ Created new activity metrics table with {metrics_records} records")
            else:
                print("No activity metrics to save")
        
    except Exception as e:
        print(f"❌ Error with Activity Metrics Delta merge: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("Personal Repository Bronze to Silver Transformation Complete!")
print("Processing Date: {}".format(PROCESSING_DATE))
print("\nSummary:")

if repos_final_df is not None:
    total_repos = repos_final_df.count()
    active_repos = repos_final_df.filter(F.col("is_active")).count()
    print(f"   Repositories processed: {total_repos}")
    print(f"   Active repositories: {active_repos}")

if activity_metrics_df is not None:
    total_metrics = activity_metrics_df.count()
    print(f"   Activity metrics calculated: {total_metrics}")


print("\nData ready for Gold layer analytics!")

# COMMAND ----------
# Clean up
spark.stop()
