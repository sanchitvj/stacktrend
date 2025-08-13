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
# Import required libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, MapType
import os
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
    """Install and import stacktrend package strictly inside a function to satisfy linters."""
    try:
        # Try different installation approaches
        github_read_token = os.environ.get("GITHUB_READ_TOKEN")
        repo_ref = os.environ.get("STACKTREND_GIT_REF", "dev")
        
        install_attempts = []
        
        # Attempt 1: Direct branch reference (most reliable)
        if github_read_token:
            install_attempts.append(f"git+https://{github_read_token}@github.com/sanchitvj/stacktrend.git@{repo_ref}")
        install_attempts.append(f"git+https://github.com/sanchitvj/stacktrend.git@{repo_ref}")
        
        # Attempt 2: Main branch fallback
        if github_read_token:
            install_attempts.append(f"git+https://{github_read_token}@github.com/sanchitvj/stacktrend.git@main")
        install_attempts.append("git+https://github.com/sanchitvj/stacktrend.git@main")
        
        for i, install_url in enumerate(install_attempts):
            try:
                print(f"Attempt {i+1}: Installing from {install_url}")
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", "--upgrade", "--no-cache-dir", install_url
                ], timeout=300)  # 5 minute timeout
                print("✅ Successfully installed stacktrend package")
                break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                print(f"❌ Attempt {i+1} failed: {e}")
                if i == len(install_attempts) - 1:
                    raise Exception("All installation attempts failed")
                continue

        from stacktrend.utils.llm_classifier import (
            LLMRepositoryClassifier as _LLMRepositoryClassifier,
            create_repository_data_from_dict as _create_repository_data_from_dict,
        )
        from stacktrend.config.settings import settings as _settings

        return _LLMRepositoryClassifier, _create_repository_data_from_dict, _settings
    
    except Exception as e:
        print(f"❌ CRITICAL: Failed to install stacktrend package: {e}")
        print("LLM classification is REQUIRED and cannot proceed without the package")
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
    Use LLM to classify repositories into smart categories
    """
    LLMRepositoryClassifier, create_repository_data_from_dict, settings = ensure_stacktrend_imports()
    
    try:
        # Convert Spark DataFrame to list of repository data
        repo_data_list = []
        repos_collected = repositories_df.collect()
        
        for row in repos_collected:
            repo_data = create_repository_data_from_dict({
                'repository_id': row['repository_id'],
                'name': row['name'],
                'full_name': row['full_name'],
                'description': row.get('description'),
                'topics': row.get('topics', []),
                'language': row.get('language'),
                'stargazers_count': row.get('stargazers_count', 0)
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
# Read Bronze data - from table
try:
    # Read from the github_repositories table (cross-lakehouse reference)
    bronze_df = spark.table("stacktrend_bronze_lh.github_repositories")
    
    # Check if we have any data at all
    total_records = bronze_df.count()
    print(f"Total records in Bronze layer: {total_records}")
    
    if total_records > 0:
        # Try to get the latest partition date
        available_dates = (bronze_df
                          .select("partition_date")
                          .distinct()
                          .orderBy(F.desc("partition_date"))
                          .collect())
        
        if available_dates:
            latest_date = available_dates[0]["partition_date"]
            bronze_df = bronze_df.where(F.col("partition_date") == latest_date)
            record_count = bronze_df.count()
            print(f"Using data from date: {latest_date} ({record_count} records)")
        else:
            # No partition dates found, use all data
            record_count = total_records
            print(f"No partition dates found, using all {record_count} records")
    else:
        raise Exception("No data available in Bronze layer")
            
except Exception as e:
    print(f"Error loading Bronze data: {e}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Cleaning and Standardization

# COMMAND ----------
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

# Write to Silver lakehouse using Delta format
try:
    (final_silver_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date", "technology_category")
     .option("path", "Tables/silver_repositories")
     .saveAsTable("stacktrend_silver_lh.silver_repositories"))
    
    print(f"Successfully wrote {clean_records} records to Silver layer")
    
except Exception as e:
    print(f"Error writing to Silver layer: {e}")
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