"""
Silver to Gold Analytics Transformation Notebook
Microsoft Fabric Notebook for generating analytics and metrics from Silver layer data

This notebook transforms clean Silver layer data into analytics-ready datasets
in the Gold layer, including technology momentum scores, trend analysis, and rankings.

Usage in Fabric:
1. Import this file as a new notebook
2. Attach to a Spark compute
3. Run cells to generate analytics
4. Schedule via Data Factory pipeline
"""

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver to Gold Analytics Transformation
# MAGIC 
# MAGIC This notebook processes clean Silver layer data and generates analytics-ready datasets for the Gold layer.
# MAGIC 
# MAGIC ## Analytics Generated:
# MAGIC 1. **Technology Momentum Scores**: Daily momentum based on growth and activity
# MAGIC 2. **Adoption Lifecycle Classification**: Emerging, growing, mature, declining stages
# MAGIC 3. **Community Health Indices**: Repository health and sustainability metrics
# MAGIC 4. **Trend Analysis**: Growth patterns and velocity measurements
# MAGIC 5. **Comparative Rankings**: Technology popularity and growth rankings

# COMMAND ----------
# Import required libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("Silver_to_Gold_Analytics").getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------
# Configuration - Use explicit lakehouse references (independent of attachments)
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
ANALYSIS_WINDOW_DAYS = 30


# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Loading from Silver Layer

# COMMAND ----------
# Read Silver layer data from table
try:
    # Read from the silver_repositories table (cross-lakehouse reference)
    silver_df = spark.table("stacktrend_silver_lh.silver_repositories")
    
    # Check if we have any data at all
    total_records = silver_df.count()
    print(f"Total records in Silver layer: {total_records}")
    
    if total_records > 0:
        # Try to get the latest partition date
        available_dates = (silver_df
                          .select("partition_date")
                          .distinct()
                          .orderBy(F.desc("partition_date"))
                          .collect())
        
        if available_dates:
            latest_date = available_dates[0]["partition_date"]
            silver_df = silver_df.where(F.col("partition_date") == latest_date)
            record_count = silver_df.count()
            print(f"Using data from date: {latest_date} ({record_count} records)")
        else:
            # No partition dates found, use all data
            record_count = total_records
            print(f"No partition dates found, using all {record_count} records")
    else:
        raise Exception("No Silver data available")
            
except Exception as e:
    print(f"Error loading Silver data: {e}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Technology Momentum Score Calculation

# COMMAND ----------
# Calculate technology-level aggregations
technology_metrics_df = (silver_df
    .groupBy("technology_category")
    .agg(
        # Repository counts
        F.count("repository_id").alias("total_repositories"),
        F.sum("stargazers_count").alias("total_stars"),
        F.sum("forks_count").alias("total_forks"),
        F.sum("watchers_count").alias("total_watchers"),
        
        # Averages
        F.avg("stargazers_count").alias("avg_stars_per_repo"),
        F.avg("forks_count").alias("avg_forks_per_repo"),
        F.avg("community_health_score").alias("avg_community_health"),
        F.avg("quality_score").alias("avg_quality_score"),
        F.avg("star_velocity_30d").alias("avg_star_velocity"),
        F.avg("commit_frequency_30d").alias("avg_commit_frequency"),
        
        # Activity metrics
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_repositories"),
        F.avg("days_since_creation").alias("avg_repository_age_days"),
        
        # License diversity
        F.countDistinct("license_category").alias("license_diversity_count")
    )
    .withColumn("active_repositories_percentage", 
                (F.col("active_repositories") / F.col("total_repositories") * 100))
)

print("Calculated technology-level aggregations")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Momentum Score Algorithm

# COMMAND ----------
# Define momentum score calculation
def calculate_momentum_score(df):
    """Calculate momentum score (0-100) based on multiple factors."""
    return (df
        .withColumn("popularity_score", 
                   F.least(F.log10(F.greatest(F.col("total_stars"), F.lit(1))) * 10, F.lit(40)))
        
        .withColumn("growth_score", 
                   F.least(F.col("avg_star_velocity") * 100, F.lit(30)))
        
        .withColumn("health_score_weighted", 
                   F.col("avg_community_health") * 0.3)
        
        .withColumn("momentum_score",
                   (F.col("popularity_score") + 
                    F.col("growth_score") + 
                    F.col("health_score_weighted")).cast(DoubleType()))
        
        .drop("popularity_score", "growth_score", "health_score_weighted")
    )

technology_with_momentum_df = calculate_momentum_score(technology_metrics_df)

print("Calculated momentum scores")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Adoption Lifecycle Classification

# COMMAND ----------
# Define adoption lifecycle classification
def classify_adoption_lifecycle(df):
    """Classify technologies into adoption lifecycle stages."""
    return (df
        .withColumn("lifecycle_stage",
            F.when((F.col("avg_star_velocity") > 1.0) & (F.col("avg_repository_age_days") < 730), "emerging")
            .when((F.col("avg_star_velocity") > 0.5) & (F.col("total_repositories") >= 5), "growing") 
            .when((F.col("total_repositories") >= 10) & (F.col("avg_repository_age_days") > 1095), "mature")
            .when(F.col("avg_star_velocity") < 0.1, "declining")
            .otherwise("stable"))
        
        .withColumn("momentum_trend",
            F.when(F.col("avg_star_velocity") > 0.5, "rising")
            .when(F.col("avg_star_velocity") > 0.1, "stable")
            .otherwise("declining"))
    )

final_technology_metrics_df = classify_adoption_lifecycle(technology_with_momentum_df)

print("Classified adoption lifecycles")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Rankings and Comparative Analysis

# COMMAND ----------
# Add rankings
ranked_metrics_df = (final_technology_metrics_df
    .withColumn("popularity_rank", 
               F.row_number().over(Window.orderBy(F.desc("total_stars"))))
    .withColumn("growth_rank",
               F.row_number().over(Window.orderBy(F.desc("avg_star_velocity"))))
    .withColumn("health_rank",
               F.row_number().over(Window.orderBy(F.desc("avg_community_health"))))
    .withColumn("momentum_rank",
               F.row_number().over(Window.orderBy(F.desc("momentum_score"))))
    .withColumn("overall_rank",
               F.row_number().over(Window.orderBy(F.desc("momentum_score"))))
)

print("Added comparative rankings")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Risk Assessment Metrics

# COMMAND ----------
# Calculate risk indicators
risk_metrics_df = (ranked_metrics_df
    .withColumn("single_maintainer_risk",
               F.when(F.col("total_repositories") <= 2, 100.0)
               .when(F.col("total_repositories") <= 5, 60.0)
               .when(F.col("total_repositories") <= 10, 30.0)
               .otherwise(10.0))
    
    .withColumn("license_diversity_score",
               F.least(F.col("license_diversity_count") * 20, F.lit(100)).cast(DoubleType()))
    
    .withColumn("sustainability_score",
               (F.col("active_repositories_percentage") * 0.4 +
                F.col("avg_community_health") * 0.3 +
                (100 - F.col("single_maintainer_risk")) * 0.3).cast(DoubleType()))
)

print("Calculated risk assessment metrics")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Gold Dataset Preparation

# COMMAND ----------
# Select final columns for Gold layer
final_gold_df = risk_metrics_df.select(
    F.col("technology_category").alias("technology_name"),
    F.lit("technology").alias("technology_category"),  # All entries are technology categories
    F.col("lifecycle_stage").alias("adoption_lifecycle"),
    F.lit(PROCESSING_DATE).alias("measurement_date"),
    
    # Popularity Metrics
    "total_repositories",
    "total_stars", 
    "total_forks",
    "avg_stars_per_repo",
    
    # Growth Metrics
    "avg_star_velocity",
    F.lit(0.0).alias("star_velocity_7d"),  # Placeholder - would need historical data
    F.col("avg_star_velocity").alias("star_velocity_30d"),
    F.lit(0.0).alias("star_velocity_90d"),  # Placeholder
    F.lit(0.0).alias("repository_growth_rate"),  # Placeholder
    
    # Community Health Metrics
    "avg_community_health",
    "active_repositories_percentage",
    F.col("total_repositories").alias("average_contributors_per_repo"),  # Approximation
    
    # Momentum and Trend
    "momentum_score",
    "momentum_trend",
    
    # Risk Indicators
    "single_maintainer_risk",
    "license_diversity_score", 
    "avg_repository_age_days",
    
    # Rankings
    "popularity_rank",
    "growth_rank",
    "health_rank",
    "overall_rank",
    
    # Metadata
    F.lit(PROCESSING_DATE).alias("partition_date")
)

print(f"Prepared {final_gold_df.count():,} technology metrics for Gold layer")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Gold Layer

# COMMAND ----------
# Write to Gold lakehouse as table
try:
    (final_gold_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date")
     .option("path", "Tables/gold_technology_metrics")
     .saveAsTable("stacktrend_gold_lh.gold_technology_metrics"))
    
    gold_record_count = final_gold_df.count()
    print(f"Successfully wrote {gold_record_count} records to Gold layer")
    
except Exception as e:
    print(f"Error writing to Gold layer: {e}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary and Analytics Preview

# COMMAND ----------
# Display analytics summary
print(f"Processing Date: {PROCESSING_DATE}")
print(f"Technologies Analyzed: {final_gold_df.count()}")

# Show top technologies by momentum
print("Top Technologies by Momentum Score:")
(final_gold_df
 .select("technology_name", "momentum_score", "adoption_lifecycle", "total_repositories", "total_stars")
 .orderBy(F.desc("momentum_score"))
 .show(5, truncate=False))

print("Silver to Gold analytics transformation completed successfully")