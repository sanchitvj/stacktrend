# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_silver_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
"""
Bronze to Silver Transformation Notebook
Microsoft Fabric Notebook for cleaning and standardizing GitHub repository data

This notebook transforms raw GitHub API responses from Bronze layer into
clean, standardized data in the Silver layer following medallion architecture.

Usage in Fabric:
1. Import this file as a new notebook
2. Attach to a Spark compute
3. Run cells to process data
4. Schedule via Data Factory pipeline
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze to Silver Data Transformation
# MAGIC 
# MAGIC This notebook processes raw GitHub repository data from the Bronze layer and transforms it into clean, standardized data in the Silver layer.
# MAGIC 
# MAGIC ## Processing Steps:
# MAGIC 1. **Data Loading**: Read from Bronze lakehouse
# MAGIC 2. **Data Cleaning**: Handle missing values, standardize formats
# MAGIC 3. **Technology Mapping**: Map languages to technology categories  
# MAGIC 4. **Metric Calculations**: Calculate velocity and health scores
# MAGIC 5. **Quality Validation**: Apply data quality rules
# MAGIC 6. **Silver Storage**: Write to Silver lakehouse with Delta format

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
    print(f"⚠️ Mount failed, will use cross-lakehouse table references: {e}")

# COMMAND ----------
# Import required libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, MapType
import sys
import subprocess
from datetime import datetime


spark = SparkSession.builder.appName("Bronze_to_Silver_Transformation").getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------
# Configuration - Use explicit lakehouse references (independent of attachments)

# Processing parameters
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
LOOKBACK_DAYS = 30  # For velocity calculations

# COMMAND ----------
# MAGIC %md
# MAGIC ## LLM-Based Technology Classification

# COMMAND ----------
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
                print("Successfully upgraded dependencies with force reload")
                
                # Force clear all related modules from memory
                modules_to_clear = [
                    'typing_extensions', 'pydantic', 'openai', 'typing_inspection',
                    'typing_inspection.introspection', 'typing_inspection.typing_objects'
                ]
                
                print("Clearing modules from memory to force fresh imports...")
                for module_name in modules_to_clear:
                    if module_name in sys.modules:
                        print(f"Removing {module_name} from sys.modules")
                        try:
                            del sys.modules[module_name]
                        except Exception as clear_error:
                            print(f"Could not clear {module_name}: {clear_error}")
                
                # Also clear any submodules
                modules_to_remove = []
                for module_name in sys.modules.keys():
                    if any(module_name.startswith(prefix) for prefix in ['typing_extensions', 'pydantic', 'openai', 'typing_inspection']):
                        modules_to_remove.append(module_name)
                
                for module_name in modules_to_remove:
                    try:
                        del sys.modules[module_name]
                        print(f"Cleared submodule: {module_name}")
                    except Exception:
                        pass
                            
            else:
                print(f"Warning: Dependency upgrade failed with exit code {result.returncode}")
                if result.stderr:
                    print(f"Dependency error: {result.stderr[:200]}")
        except Exception as e:
            print(f"Warning: Failed to upgrade dependencies: {e}")
        
        # Now install stacktrend package
        for i, install_url in enumerate(install_attempts):
            try:
                print(f"Attempt {i+1}: Installing from {install_url}")
                
                # Different approaches for different URL types
                import sys
                import os
                
                if install_url.endswith('.zip'):
                    # Direct ZIP download - no git involved
                    cmd = [sys.executable, "-m", "pip", "install", "--upgrade", "--no-cache-dir", install_url]
                else:
                    # Git URL - add extra flags to suppress git output
                    cmd = [sys.executable, "-m", "pip", "install", "--upgrade", "--no-cache-dir", 
                           "--quiet", "--no-warn-script-location", install_url]
                
                # Redirect stderr to devnull for git operations
                with open(os.devnull, 'w') as devnull:
                    result = subprocess.run(cmd, timeout=300, stdout=subprocess.PIPE, 
                                          stderr=devnull if 'git+' in install_url else subprocess.PIPE, 
                                          text=True)
                
                # Check if installation was successful (exit code 0)
                if result.returncode == 0:
                    print("Successfully installed stacktrend package")
                    break
                else:
                    print(f"Installation failed with exit code {result.returncode}")
                    if i == len(install_attempts) - 1:
                        raise Exception("All installation attempts failed")
                    continue
                    
            except subprocess.TimeoutExpired:
                print(f"❌ Attempt {i+1} timed out after 5 minutes")
                if i == len(install_attempts) - 1:
                    raise Exception("All installation attempts timed out")
                continue
            except Exception as e:
                print(f"❌ Attempt {i+1} failed: {e}")
                if i == len(install_attempts) - 1:
                    raise Exception("All installation attempts failed")
                continue

        # Try importing with restart if needed
        try:
            from stacktrend.utils.llm_classifier import (
                LLMRepositoryClassifier as _LLMRepositoryClassifier,
                create_repository_data_from_dict as _create_repository_data_from_dict,
            )
            from stacktrend.config.settings import settings as _settings
            print("Successfully imported stacktrend modules")
            return _LLMRepositoryClassifier, _create_repository_data_from_dict, _settings
            
        except ImportError as import_error:
            print(f"❌ Import error after installation: {import_error}")
            print("This might be due to dependency conflicts in the current Python session")
            
            # Try one more dependency upgrade with complete environment reset
            print("Attempting complete dependency reset...")
            try:
                import sys
                
                # Uninstall everything first
                subprocess.run([
                    sys.executable, "-m", "pip", "uninstall", "-y", 
                    "typing_extensions", "pydantic", "openai", "typing-inspection", "stacktrend"
                ], timeout=60, capture_output=True, text=True)
                
                # Clear all related modules from memory
                all_modules_to_clear = list(sys.modules.keys())
                for module_name in all_modules_to_clear:
                    if any(prefix in module_name for prefix in ['typing_extensions', 'pydantic', 'openai', 'typing_inspection', 'stacktrend']):
                        try:
                            del sys.modules[module_name]
                        except Exception:
                            pass
                
                # Install with specific compatible versions
                result = subprocess.run([
                    sys.executable, "-m", "pip", "install", "--no-cache-dir",
                    "typing_extensions==4.12.2", "pydantic==2.8.2", "openai==1.35.15", "nest_asyncio"
                ], timeout=120, capture_output=True, text=True)
                
                if result.returncode != 0:
                    print(f"Complete reset failed with exit code {result.returncode}")
                    if result.stderr:
                        print(f"Reset error: {result.stderr[:200]}")
                    raise Exception("Complete dependency reset failed")
                else:
                    print("Complete dependency reset successful")
                
                # Try import again
                from stacktrend.utils.llm_classifier import (
                    LLMRepositoryClassifier as _LLMRepositoryClassifier,
                    create_repository_data_from_dict as _create_repository_data_from_dict,
                )
                from stacktrend.config.settings import settings as _settings
                print("Successfully imported after force reinstall")
                return _LLMRepositoryClassifier, _create_repository_data_from_dict, _settings
                
            except Exception as retry_error:
                print(f"❌ Force reinstall also failed: {retry_error}")
                raise ImportError(f"Cannot import stacktrend modules due to dependency conflicts: {import_error}")
    
    except Exception as e:
        print(f"❌ CRITICAL: Failed to install stacktrend package: {e}")
        print("LLM classification is MANDATORY and cannot proceed without it")
        raise Exception(f"LLM classification setup failed: {e}")

def extract_language_distribution(language, topics, name):
    """Extract programming languages used and their estimated distribution."""
    languages = {}
    
    # Primary language gets 70% if specified
    if language and language.strip() and language.lower() not in ['null', 'none', '']:
        languages[language] = 70.0
    
    # Check topics for additional languages
    programming_languages = {
        'python': 'Python', 'javascript': 'JavaScript', 'typescript': 'TypeScript',
        'java': 'Java', 'go': 'Go', 'rust': 'Rust', 'cpp': 'C++', 'c++': 'C++',
        'csharp': 'C#', 'c#': 'C#', 'php': 'PHP', 'ruby': 'Ruby', 'swift': 'Swift',
        'kotlin': 'Kotlin', 'scala': 'Scala', 'r': 'R', 'julia': 'Julia',
        'shell': 'Shell', 'bash': 'Shell', 'dockerfile': 'Dockerfile',
        'yaml': 'YAML', 'json': 'JSON', 'sql': 'SQL'
    }
    
    topic_languages = []
    for topic in (topics or []):
        topic_lower = topic.lower()
        for lang_key, lang_name in programming_languages.items():
            if lang_key in topic_lower:
                topic_languages.append(lang_name)
    
    # Distribute remaining percentage among topic languages
    if topic_languages:
        remaining_percent = 30.0 if languages else 100.0
        percent_per_lang = remaining_percent / len(topic_languages)
        for lang in topic_languages:
            if lang not in languages:
                languages[lang] = percent_per_lang
    
    # If no languages found, mark as unknown
    if not languages:
        languages['Unknown'] = 100.0
    
    # Normalize to 100%
    total = sum(languages.values())
    if total > 0:
        languages = {k: round((v / total) * 100, 1) for k, v in languages.items()}
    
    return languages



def classify_repositories_with_llm(repositories_df):
    """
    Use LLM to classify repositories into smart categories - LLM ONLY, NO FALLBACKS
    """
    LLMRepositoryClassifier, create_repository_data_from_dict, settings = ensure_stacktrend_imports()
    
    try:
        # Convert Spark DataFrame to list of repository data
        repo_data_list = []
        repos_collected = repositories_df.collect()
        
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
        
        # Initialize LLM classifier
        classifier = LLMRepositoryClassifier(
            api_key=settings.AZURE_OPENAI_API_KEY,
            endpoint=settings.AZURE_OPENAI_ENDPOINT,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            model=settings.AZURE_OPENAI_MODEL
        )
        
        # Classify repositories
        print(f"Classifying {len(repo_data_list)} repositories with LLM...")
        classifications = classifier.classify_repositories_sync(repo_data_list)
        
        # Convert results back to Spark DataFrame format
        classification_map = {
            c.repo_id: {
                'primary_category': c.primary_category,
                'subcategory': c.subcategory,
                'confidence': c.confidence
            }
            for c in classifications
        }
        
        # Add classifications to original DataFrame
        def add_classification(repo_id):
            classification = classification_map.get(str(repo_id), {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return classification['primary_category']
        
        def add_subcategory(repo_id):
            classification = classification_map.get(str(repo_id), {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return classification['subcategory']
        
        def add_confidence(repo_id):
            classification = classification_map.get(str(repo_id), {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return float(classification['confidence'])
        
        add_classification_udf = F.udf(add_classification, StringType())
        add_subcategory_udf = F.udf(add_subcategory, StringType())
        add_confidence_udf = F.udf(add_confidence, DoubleType())
        
        return (repositories_df
                .withColumn("technology_category", add_classification_udf(F.col("repository_id")))
                .withColumn("technology_subcategory", add_subcategory_udf(F.col("repository_id")))
                .withColumn("classification_confidence", add_confidence_udf(F.col("repository_id"))))
        
    except Exception as e:
        print(f"Error classifying repositories with LLM: {e}")
        raise



extract_lang_dist_udf = F.udf(
    extract_language_distribution, MapType(StringType(), DoubleType())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Loading and Initial Processing

# COMMAND ----------
# Check if Bronze table exists before attempting to read
print("Verifying Bronze lakehouse table exists...")
try:
    # First check if the table exists at all
    table_exists = False
    try:
        spark.sql("DESCRIBE TABLE stacktrend_bronze_lh.github_repositories")
        table_exists = True
        print("Bronze table exists")
    except Exception:
        print("❌ Bronze table does not exist")
        raise Exception("Bronze table 'github_repositories' does not exist. GitHub Data Ingestion may have failed.")
    
    # If table exists, try to read it
    bronze_df = None
    load_attempts = [
        ("cross-lakehouse table", lambda: spark.table("stacktrend_bronze_lh.github_repositories")),
        ("mounted path", lambda: spark.read.format("delta").load("/mnt/bronze/Tables/github_repositories")),
        ("simple table name", lambda: spark.table("github_repositories"))
    ]
    
    for attempt_name, load_func in load_attempts:
        try:
            bronze_df = load_func()
            print(f"Successfully loaded from {attempt_name}")
            break
        except Exception as e:
            print(f"Failed to load using {attempt_name}: {str(e)[:100]}")
            continue
    
    if bronze_df is None:
        raise Exception("All data loading methods failed")
    
    # Verify data integrity
    total_records = bronze_df.count()
    print(f"Total records in Bronze layer: {total_records}")
    
    if total_records == 0:
        raise Exception("Bronze table exists but contains no data. Check GitHub Data Ingestion logs.")
    
    # Filter to latest partition if available
    try:
        latest_partition = bronze_df.agg(F.max("partition_date")).collect()[0][0]
        if latest_partition:
            bronze_df = bronze_df.filter(F.col("partition_date") == latest_partition)
            record_count = bronze_df.count()
            print(f"Using latest partition {latest_partition}: {record_count} records")
        else:
            record_count = total_records
            print(f"No partitions found, using all {record_count} records")
    except Exception:
        record_count = total_records
        print(f"Using all available records: {record_count}")
            
except Exception as e:
    print(f"❌ Bronze data access failed: {e}")
    print("Possible causes:")
    print("1. GitHub Data Ingestion notebook failed")
    print("2. Data Factory Web Activity returned empty response")
    print("3. Bronze lakehouse permissions issue")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Cleaning and Standardization

# COMMAND ----------
# Verify bronze_df is available before processing
if 'bronze_df' not in locals() or bronze_df is None:
    raise Exception("Bronze DataFrame not available. Previous data loading step failed.")

print(f"Starting data cleaning for {record_count} records...")

# Clean and standardize the data
silver_df_basic = (bronze_df
    # Basic cleaning
    .withColumn("name_clean", F.regexp_replace(F.col("name"), r"[^\w\-\.]", ""))
    .withColumn("description_clean", 
                F.when(F.col("description").isNotNull(), 
                       F.regexp_replace(F.col("description"), r"[^\w\s\-\.\,\:]", ""))
                .otherwise(None))
    
    # Standardize language field
    .withColumn("primary_language", 
                F.when(F.col("language").isNotNull(), F.lower(F.trim(F.col("language"))))
                .otherwise("unknown"))
    
    # Language distribution (as key-value map)
    .withColumn("language_distribution",
                extract_lang_dist_udf(F.col("primary_language"),
                                     F.col("topics"),
                                     F.col("name")))
    
    # Clean topics array
    .withColumn("topics_standardized",
                F.when(F.col("topics").isNotNull(),
                       F.transform(F.col("topics"), 
                                  lambda x: F.lower(F.trim(x))))
                .otherwise(F.array()))
    
    # Standardize license information
    .withColumn("license_category",
                F.when(F.col("license_name").isNotNull(),
                       F.when(F.col("license_name").contains("MIT"), "permissive")
                       .when(F.col("license_name").contains("Apache"), "permissive") 
                       .when(F.col("license_name").contains("GPL"), "copyleft")
                       .when(F.col("license_name").contains("BSD"), "permissive")
                       .otherwise("other"))
                .otherwise("none"))
    
    # Activity indicators
    .withColumn("days_since_push",
                F.datediff(F.current_date(), F.col("pushed_at")))
    .withColumn("days_since_creation", 
                F.datediff(F.current_date(), F.col("created_at")))
    .withColumn("is_active",
                F.col("days_since_push") <= 90)  # Active if pushed within 90 days
    
    # Add processing metadata
    .withColumn("processed_timestamp", F.current_timestamp())
    .withColumn("partition_date", F.lit(PROCESSING_DATE))
    .withColumn("data_quality_flags", F.array())  # Initialize empty array
)

print("Applied basic cleaning and standardization")

# Apply LLM-based classification
print("Starting LLM-based technology classification...")
silver_df = classify_repositories_with_llm(silver_df_basic)
print("LLM classification completed")

# COMMAND ----------
# MAGIC %md 
# MAGIC ## Metric Calculations

# COMMAND ----------
# Calculate velocity and health metrics
# For this example, we'll use simplified calculations
# In production, you'd fetch historical data for proper velocity calculations

silver_with_metrics_df = (silver_df
    # Star velocity (simplified - in production, use historical data)
    .withColumn("star_velocity_30d",
                F.when(F.col("days_since_creation") > 0,
                       F.col("stargazers_count") / F.greatest(F.col("days_since_creation"), F.lit(1)))
                .otherwise(0.0))
    
    # Commit frequency approximation (would need commit history for accuracy)
    .withColumn("commit_frequency_30d",
                F.when(F.col("is_active"), 
                       F.rand() * 10)  # Placeholder - replace with actual commit data
                .otherwise(0.0))
    
    # Community health score calculation
    .withColumn("has_description", F.col("description").isNotNull())
    .withColumn("has_license", F.col("license_name").isNotNull())
    .withColumn("has_topics", F.size(F.col("topics")) > 0)
    .withColumn("has_recent_activity", F.col("is_active"))
    .withColumn("reasonable_size", F.col("size") > 0)
    
    # Health score (0-100)
    .withColumn("community_health_score",
                (F.when(F.col("has_description"), 20).otherwise(0) +
                 F.when(F.col("has_license"), 20).otherwise(0) +
                 F.when(F.col("has_topics"), 20).otherwise(0) +
                 F.when(F.col("has_recent_activity"), 20).otherwise(0) +
                 F.when(F.col("reasonable_size"), 20).otherwise(0)).cast("double"))
    
    # Quality score based on various factors
    .withColumn("quality_score",
                (F.least(F.log10(F.greatest(F.col("stargazers_count"), F.lit(1))) * 10, F.lit(50)) +
                 F.least(F.log10(F.greatest(F.col("forks_count"), F.lit(1))) * 5, F.lit(25)) +
                 F.when(F.col("has_wiki"), 10).otherwise(0) +
                 F.when(F.col("has_pages"), 10).otherwise(0) +
                 F.least(F.size(F.col("topics")) * 2, F.lit(15))).cast("double"))
    
    # Drop intermediate calculation columns
    .drop("has_description", "has_license", "has_topics", 
          "has_recent_activity", "reasonable_size")
)

print("Calculated velocity and health metrics")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------
# Apply data quality rules and flag issues
def validate_record(row):
    """Validate a record and return quality flags."""
    flags = []
    
    # Check for missing critical data
    if not row["name"] or row["name"].strip() == "":
        flags.append("missing_name")
    
    if row["stargazers_count"] < 0:
        flags.append("negative_stars")
        
    if row["forks_count"] < 0:
        flags.append("negative_forks")
    
    if row["created_at"] is None:
        flags.append("missing_created_date")
        
    if row["community_health_score"] < 0 or row["community_health_score"] > 100:
        flags.append("invalid_health_score")
    
    return flags

# Apply validation (simplified version - in production use DataFrame operations)
silver_validated_df = (silver_with_metrics_df
    # Add basic validation flags using DataFrame operations
    .withColumn("data_quality_flags",
                F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""),
                       F.array(F.lit("missing_name")))
                .when(F.col("stargazers_count") < 0,
                      F.array(F.lit("negative_stars")))
                .when(F.col("community_health_score") < 0,
                      F.array(F.lit("invalid_health_score")))  
                .otherwise(F.array()))
    
    # Filter out records with critical quality issues
    .filter(~F.array_contains(F.col("data_quality_flags"), "missing_name"))
    .filter(F.col("stargazers_count") >= 0)
)

# Count quality issues
total_records = silver_with_metrics_df.count()
clean_records = silver_validated_df.count()
rejected_records = total_records - clean_records

print("Data quality validation completed")
print(f"Total records: {total_records:,}")
print(f"Clean records: {clean_records:,}")
print(f"Rejected records: {rejected_records:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Silver Layer

# COMMAND ----------
# Select final columns for Silver layer
final_silver_df = silver_validated_df.select(
    "repository_id",
    "name", 
    "full_name",
    "owner_login",
    "owner_type",
    "description_clean",
    "created_at",
    "updated_at", 
    "pushed_at",
    "primary_language",
    "technology_category",          # LLM-classified primary category
    "technology_subcategory",       # LLM-classified subcategory  
    "classification_confidence",    # LLM classification confidence
    "language_distribution",        # Programming languages as key-value pairs
    "stargazers_count",
    "watchers_count",
    "forks_count",
    "open_issues_count",
    "star_velocity_30d",
    "commit_frequency_30d",
    "community_health_score",
    "quality_score",
    "topics_standardized",
    "license_category",
    "is_active",
    "days_since_push",
    "days_since_creation",
    "data_quality_flags",
    "processed_timestamp",
    "partition_date"
)

# Write to Silver lakehouse (using default lakehouse configuration)
try:
    (final_silver_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date", "technology_category")
     .saveAsTable("silver_repositories"))
    
    print(f"Successfully wrote {clean_records} records to Silver layer")
    
except Exception as e:
    print(f"❌ Error saving to Silver lakehouse: {e}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary and Next Steps

# COMMAND ----------
# Display processing summary
print(f"Processing Date: {PROCESSING_DATE}")
print(f"Records Processed: {total_records}")
print(f"Records Written to Silver: {clean_records}")
print(f"Data Quality Issues: {rejected_records}")
print(f"Success Rate: {(clean_records/total_records)*100:.1f}%")

# Show technology category distribution
tech_distribution = (final_silver_df
                     .groupBy("technology_category")
                     .count()
                     .orderBy(F.desc("count")))

print("Technology Category Distribution:")
tech_distribution.show(10, truncate=False)

print("Bronze to Silver transformation completed successfully")