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
from pyspark.sql.types import StringType
from datetime import datetime

# Initialize Spark Session (for Fabric notebooks)
spark = SparkSession.builder.appName("Bronze_to_Silver_Transformation").getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------
# Configuration - Use attached lakehouses

# Processing parameters
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
LOOKBACK_DAYS = 30  # For velocity calculations

print(f"Processing date: {PROCESSING_DATE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Technology Taxonomy Mapping

# COMMAND ----------
# Define technology category mapping
TECHNOLOGY_CATEGORIES = {
    # Programming Languages
    "python": "programming_language",
    "javascript": "programming_language", 
    "typescript": "programming_language",
    "java": "programming_language",
    "c#": "programming_language",
    "go": "programming_language",
    "rust": "programming_language",
    "kotlin": "programming_language",
    "swift": "programming_language",
    "php": "programming_language",
    "ruby": "programming_language",
    "c++": "programming_language",
    "c": "programming_language",
    "scala": "programming_language",
    "r": "programming_language",
    
    # Frameworks & Libraries
    "react": "frontend_library",
    "vue": "frontend_library", 
    "angular": "frontend_library",
    "svelte": "frontend_library",
    "django": "backend_framework",
    "flask": "backend_framework",
    "fastapi": "backend_framework",
    "express": "backend_framework",
    "spring": "backend_framework",
    "laravel": "backend_framework",
    
    # Data & AI/ML
    "tensorflow": "ai_ml_library",
    "pytorch": "ai_ml_library",
    "scikit-learn": "ai_ml_library",
    "pandas": "data_tool",
    "numpy": "data_tool",
    "spark": "data_tool",
    "kafka": "data_tool",
    
    # Databases
    "postgresql": "database",
    "mysql": "database",
    "mongodb": "database",
    "redis": "database",
    "elasticsearch": "database",
    
    # DevOps
    "docker": "devops_tool",
    "kubernetes": "devops_tool",
    "terraform": "devops_tool",
    "ansible": "devops_tool",
    
    # Default category
    "other": "other"
}

# Create UDF for technology categorization
def categorize_technology(language, topics, name):
    """Categorize technology based on language, topics, and repository name."""
    if not language:
        language = ""
    
    language_lower = language.lower()
    name_lower = name.lower() if name else ""
    topics_lower = [topic.lower() for topic in (topics or [])]
    
    # Check direct language mapping
    if language_lower in TECHNOLOGY_CATEGORIES:
        return TECHNOLOGY_CATEGORIES[language_lower]
    
    # Check topics for framework/library indicators
    for topic in topics_lower:
        if topic in TECHNOLOGY_CATEGORIES:
            return TECHNOLOGY_CATEGORIES[topic]
    
    # Check repository name for patterns
    for tech, category in TECHNOLOGY_CATEGORIES.items():
        if tech in name_lower:
            return category
    
    return "other"

categorize_tech_udf = F.udf(categorize_technology, StringType())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Loading and Initial Processing

# COMMAND ----------
# Read Bronze data - from table
try:
    # Read from the github_repositories table (with lakehouse prefix)
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
silver_df = (bronze_df
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
    
    # Technology categorization
    .withColumn("technology_category",
                categorize_tech_udf(F.col("primary_language"), 
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
    "technology_category",
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
     .saveAsTable("silver_repositories"))
    
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