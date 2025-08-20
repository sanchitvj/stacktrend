# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_gold_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver to Gold Analytics Transformation Notebook
# MAGIC 
# MAGIC Microsoft Fabric Notebook for generating analytics and metrics from Silver layer data
# MAGIC 
# MAGIC This notebook transforms clean Silver layer data into analytics-ready datasets
# MAGIC in the Gold layer, including technology momentum scores, trend analysis, and rankings.
# MAGIC 
# MAGIC ## Usage in Fabric:
# MAGIC 1. Import this file as a new notebook
# MAGIC 2. Attach to a Spark compute
# MAGIC 3. Run cells to generate analytics
# MAGIC 4. Schedule via Data Factory pipeline

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
# Mount additional lakehouses for cross-lakehouse access using secure context
try:
    from notebookutils import mssparkutils
    
    # Get current workspace context securely
    workspace_id = mssparkutils.env.getWorkspaceId()
    
    # Mount Silver lakehouse using lakehouse name (Fabric resolves the ID securely)
    silver_mount = "/mnt/silver"
    silver_abfs = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/stacktrend_silver_lh.Lakehouse"
    
    # Check if already mounted
    existing_mounts = [mount.mountPoint for mount in mssparkutils.fs.mounts()]
    if silver_mount not in existing_mounts:
        mssparkutils.fs.mount(silver_abfs, silver_mount)
        print(f"SUCCESS: Mounted Silver lakehouse at {silver_mount}")
    else:
        print(f"SUCCESS: Silver lakehouse already mounted at {silver_mount}")
        
except Exception as e:
    print(f"WARNING: Mount failed, will use cross-lakehouse table references: {e}")

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
    # Read from the github_curated table (cross-lakehouse reference)
    # Try mounted path first, fallback to cross-lakehouse reference
    try:
        silver_df = spark.read.format("delta").load("/mnt/silver/Tables/github_curated")
        print("SUCCESS: Successfully loaded from mounted Silver lakehouse")
    except Exception as e:
        print(f"Error loading from mounted Silver lakehouse: {e}")
        # Fallback to cross-lakehouse table reference
        silver_df = spark.table("stacktrend_silver_lh.github_curated")
        print("SUCCESS: Successfully loaded using cross-lakehouse reference")
    
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
# Write to Gold lakehouse (using default lakehouse configuration)
try:
    (final_gold_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date")
     .saveAsTable("tech_metrics"))
    
    gold_record_count = final_gold_df.count()
    print(f"SUCCESS: Successfully wrote {gold_record_count} records to Gold layer")
    
except Exception as e:
    print(f"❌ Error saving to Gold lakehouse: {e}")
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table 2: Individual Repository Rankings

# COMMAND ----------
# Create repo_ranks table - Individual repository insights
repo_ranks_df = (silver_df
    .withColumn("momentum_score", 
                F.log10(F.greatest(F.col("stargazers_count"), F.lit(1))) * 10 +
                F.col("star_velocity_30d") * 100 + 
                F.col("community_health_score") * 30)
    .withColumn("quality_rank", 
                F.row_number().over(Window.partitionBy("technology_category")
                                   .orderBy(F.desc("quality_score"))))
    .withColumn("popularity_rank",
                F.row_number().over(Window.orderBy(F.desc("stargazers_count"))))
    .withColumn("growth_rank",
                F.row_number().over(Window.orderBy(F.desc("star_velocity_30d"))))
    .select(
        "repository_id",
        "name",
        "full_name", 
        "technology_category",
        "technology_subcategory",
        "stargazers_count",
        "forks_count",
        "momentum_score",
        "quality_rank",
        "popularity_rank", 
        "growth_rank",
        "community_health_score",
        "is_active",
        F.lit(PROCESSING_DATE).alias("last_updated"),
        F.lit(PROCESSING_DATE).alias("partition_date")
    )
)

# Save repo_ranks table
try:
    (repo_ranks_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date", "technology_category")
     .saveAsTable("repo_ranks"))
    
    print(f"SUCCESS: Created repo_ranks table with {repo_ranks_df.count()} records")
except Exception as e:
    print(f"❌ Error creating repo_ranks: {e}")

# COMMAND ----------
# MAGIC %md 
# MAGIC ## Table 3: Daily Technology Trends

# COMMAND ----------
# Create trend_daily table - Daily technology trend tracking
trend_daily_df = (silver_df
    .groupBy("technology_category", "partition_date")
    .agg(
        F.count("repository_id").alias("total_repos"),
        F.sum("stargazers_count").alias("total_stars"),
        F.sum("forks_count").alias("total_forks"),
        F.avg("star_velocity_30d").alias("avg_daily_growth"),
        F.avg("community_health_score").alias("avg_health"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_repos")
    )
    .withColumn("growth_rate", F.col("avg_daily_growth") * 30)  # Monthly projection
    .withColumn("market_share", 
                F.col("total_stars") / F.sum("total_stars").over(Window.partitionBy("partition_date")) * 100)
    .withColumn("momentum_change", F.lit(0.0))  # Placeholder for historical comparison
    .withColumn("rank_change", F.lit(0))        # Placeholder for historical comparison
    .select(
        F.col("partition_date").alias("date"),
        "technology_category",
        "total_repos", 
        "total_stars",
        "avg_daily_growth",
        "growth_rate",
        "market_share",
        "momentum_change",
        "rank_change",
        "avg_health",
        F.lit(PROCESSING_DATE).alias("partition_date")
    )
)

# Save trend_daily table
try:
    (trend_daily_df
     .write
     .format("delta") 
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date")
     .saveAsTable("trend_daily"))
    
    print(f"SUCCESS: Created trend_daily table with {trend_daily_df.count()} records")
except Exception as e:
    print(f"❌ Error creating trend_daily: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table 4: Technology Health Assessment  

# COMMAND ----------
# Create tech_health table - Technology sustainability and risk
tech_health_df = (silver_df
    .groupBy("technology_category")
    .agg(
        F.avg("community_health_score").alias("health_score"),
        F.count("repository_id").alias("maintainer_count"),  # Approximation
        F.countDistinct("license_category").alias("license_diversity"),
        F.avg("quality_score").alias("avg_quality"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_projects"),
        F.avg("days_since_push").alias("avg_days_since_activity"),
        F.stddev("stargazers_count").alias("star_distribution_stddev")
    )
    .withColumn("sustainability_rating",
                F.when(F.col("health_score") >= 0.8, "excellent")
                .when(F.col("health_score") >= 0.6, "good") 
                .when(F.col("health_score") >= 0.4, "fair")
                .otherwise("poor"))
    .withColumn("risk_level",
                F.when(F.col("avg_days_since_activity") > 90, "high")
                .when(F.col("avg_days_since_activity") > 30, "medium")
                .otherwise("low"))
    .select(
        "technology_category",
        "health_score", 
        "risk_level",
        "maintainer_count",
        "license_diversity",
        "sustainability_rating",
        "active_projects",
        "avg_quality",
        F.lit(PROCESSING_DATE).alias("last_assessed"),
        F.lit(PROCESSING_DATE).alias("partition_date")
    )
)

# Save tech_health table
try:
    (tech_health_df
     .write
     .format("delta")
     .mode("overwrite") 
     .option("overwriteSchema", "true")
     .partitionBy("partition_date")
     .saveAsTable("tech_health"))
     
    print(f"SUCCESS: Created tech_health table with {tech_health_df.count()} records")
except Exception as e:
    print(f"❌ Error creating tech_health: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table 5: Programming Language Statistics

# COMMAND ----------
# Create lang_stats table - Programming language insights
lang_stats_df = (silver_df
    .select("primary_language", "stargazers_count", "forks_count", "star_velocity_30d", "is_active")
    .filter(F.col("primary_language").isNotNull())
    .groupBy("primary_language")
    .agg(
        F.count("*").alias("repo_count"),
        F.sum("stargazers_count").alias("total_stars"),
        F.avg("stargazers_count").alias("avg_stars"),
        F.avg("star_velocity_30d").alias("growth_rate"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_repos")
    )
    .withColumn("star_percentage", 
                F.col("total_stars") / F.sum("total_stars").over(Window.partitionBy()) * 100)
    .withColumn("popularity_rank",
                F.row_number().over(Window.orderBy(F.desc("total_stars"))))
    .withColumn("adoption_stage",
                F.when(F.col("growth_rate") > 1.0, "emerging")
                .when(F.col("growth_rate") > 0.3, "growing")
                .when(F.col("repo_count") >= 20, "mature") 
                .otherwise("stable"))
    .select(
        "primary_language",
        "repo_count",
        "total_stars",
        "star_percentage", 
        "growth_rate",
        "popularity_rank",
        "adoption_stage",
        "active_repos",
        F.lit(PROCESSING_DATE).alias("partition_date")
    )
)

# Save lang_stats table
try:
    (lang_stats_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true") 
     .partitionBy("partition_date")
     .saveAsTable("lang_stats"))
     
    print(f"SUCCESS: Created lang_stats table with {lang_stats_df.count()} records")
except Exception as e:
    print(f"❌ Error creating lang_stats: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table 6: Market Pulse Indicators

# COMMAND ----------
# Create market_pulse table - Market-level indicators
market_pulse_df = spark.createDataFrame([
    {
        "date": PROCESSING_DATE,
        "total_technologies": final_gold_df.count(),
        "emerging_techs": final_gold_df.filter(F.col("adoption_lifecycle") == "emerging").count(),
        "declining_techs": final_gold_df.filter(F.col("adoption_lifecycle") == "declining").count(), 
        "total_repositories": silver_df.count(),
        "total_stars": silver_df.agg(F.sum("stargazers_count")).collect()[0][0],
        "avg_momentum": final_gold_df.agg(F.avg("momentum_score")).collect()[0][0],
        "market_volatility": final_gold_df.agg(F.stddev("momentum_score")).collect()[0][0],
        "active_percentage": silver_df.filter(F.col("is_active")).count() / silver_df.count() * 100,
        "partition_date": PROCESSING_DATE
    }
])

# Save market_pulse table  
try:
    (market_pulse_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date")
     .saveAsTable("market_pulse"))
     
    print(f"SUCCESS: Created market_pulse table with {market_pulse_df.count()} records")
except Exception as e:
    print(f"❌ Error creating market_pulse: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table 7: Technology Adoption Matrix

# COMMAND ----------
# Create adoption_matrix table - Technology co-occurrence patterns
# This analyzes which technologies appear together in repository topics
adoption_matrix_df = (silver_df
    .select("technology_category", "technology_subcategory", "topics_standardized", "stargazers_count")
    .filter(F.col("topics_standardized").isNotNull())
    .filter(F.size(F.col("topics_standardized")) > 0)  # Ensure array is not empty
    .select("technology_category", "stargazers_count", F.explode("topics_standardized").alias("topic"))
    .filter(F.col("topic") != F.col("technology_category"))  # Exclude self-references
    .groupBy("technology_category", "topic")
    .agg(
        F.count("*").alias("co_occurrence_count"),
        F.sum("stargazers_count").alias("combined_stars")
    )
    .filter(F.col("co_occurrence_count") >= 3)  # Minimum co-occurrence threshold
    .withColumn("correlation_score", 
                F.log10(F.greatest(F.col("combined_stars"), F.lit(1))) * 
                F.sqrt(F.col("co_occurrence_count")))
    .withColumn("ecosystem_strength",
                F.when(F.col("correlation_score") > 10, "strong")
                .when(F.col("correlation_score") > 5, "moderate")
                .otherwise("weak"))
    .select(
        F.col("technology_category").alias("tech_primary"),
        F.col("topic").alias("tech_secondary"),
        "co_occurrence_count",
        "correlation_score",
        "ecosystem_strength",
        F.lit(PROCESSING_DATE).alias("partition_date")
    )
)

# Save adoption_matrix table
try:
    (adoption_matrix_df
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .partitionBy("partition_date")
     .saveAsTable("adoption_matrix"))
     
    print(f"SUCCESS: Created adoption_matrix table with {adoption_matrix_df.count()} records")
except Exception as e:
    print(f"❌ Error creating adoption_matrix: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Summary - All Gold Tables Created

# COMMAND ----------
# Display comprehensive analytics summary
print(f"Gold Layer Analytics Completed - {PROCESSING_DATE}")
print("=" * 50)

# Show all tables created
tables_created = [
    ("tech_metrics", final_gold_df.count()),
    ("repo_ranks", repo_ranks_df.count()),
    ("trend_daily", trend_daily_df.count()), 
    ("tech_health", tech_health_df.count()),
    ("lang_stats", lang_stats_df.count()),
    ("market_pulse", market_pulse_df.count()),
    ("adoption_matrix", adoption_matrix_df.count())
]

print("Tables Created:")
for table_name, record_count in tables_created:
    print(f"  {table_name}: {record_count:,} records")

print("\nSilver to Gold analytics transformation completed successfully")