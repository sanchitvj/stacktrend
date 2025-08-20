# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_gold_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
# MAGIC %md
# MAGIC # Personal Repository Silver to Gold Analytics
# MAGIC 
# MAGIC Microsoft Fabric Notebook for generating personal GitHub repository analytics
# MAGIC 
# MAGIC This notebook creates dashboard-ready analytics for personal repository monitoring and presentations.

# COMMAND ----------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("Personal_Repos_Silver_to_Gold_Analytics").getOrCreate()

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
# Configuration
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Loading from Silver Layer

# COMMAND ----------
# Read Silver layer portfolio data
try:
    # Try mounted path first, fallback to cross-lakehouse reference
    try:
        portfolio_silver_df = spark.read.format("delta").load("/mnt/silver/Tables/github_my_portfolio")
        print("SUCCESS: Successfully loaded portfolio from mounted Silver lakehouse")
    except Exception as e:
        print(f"Error loading from mounted Silver lakehouse: {e}")
        # Fallback to cross-lakehouse table reference
        portfolio_silver_df = spark.table("stacktrend_silver_lh.github_my_portfolio")
        print("SUCCESS: Successfully loaded portfolio using cross-lakehouse reference")
    
    # Check if we have any data
    total_records = portfolio_silver_df.count()
    print(f"Total portfolio records in Silver layer: {total_records}")
    
except Exception as e:
    print(f"❌ Error loading portfolio data: {e}")
    portfolio_silver_df = None

# Read Silver layer activity metrics
try:
    try:
        activity_silver_df = spark.read.format("delta").load("/mnt/silver/Tables/github_activity_metrics")
        print("SUCCESS: Successfully loaded activity metrics from mounted Silver lakehouse")
    except Exception as e:
        print(f"Error loading activity metrics from mounted Silver lakehouse: {e}")
        activity_silver_df = spark.table("stacktrend_silver_lh.github_activity_metrics")
        print("SUCCESS: Successfully loaded activity metrics using cross-lakehouse reference")
    
    activity_records = activity_silver_df.count()
    print(f"Total activity records in Silver layer: {activity_records}")
    
except Exception as e:
    print(f"❌ Error loading activity data: {e}")
    activity_silver_df = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Portfolio Overview Analytics

# COMMAND ----------
if portfolio_silver_df is not None:
    print("Generating portfolio overview analytics...")
    
    # Calculate overall portfolio metrics
    portfolio_overview_df = portfolio_silver_df.agg(
        F.count("repository_id").alias("total_repositories"),
        F.sum(F.when(F.col("visibility") == "public", 1).otherwise(0)).alias("public_repositories"),
        F.sum(F.when(F.col("visibility") == "private", 1).otherwise(0)).alias("private_repositories"),
        F.sum("stargazers_count").alias("total_stars"),
        F.sum("forks_count").alias("total_forks"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_repositories"),
        F.avg("quality_score").alias("avg_quality_score"),
        F.sum("repository_size_mb").alias("total_size_mb")
    )
    
    # Get top technologies and languages
    top_technologies = portfolio_silver_df.groupBy("technology_category").count().orderBy(F.desc("count")).limit(5)
    top_languages = portfolio_silver_df.filter(F.col("primary_language").isNotNull()).groupBy("primary_language").count().orderBy(F.desc("count")).limit(5)
    
    # Convert to arrays
    tech_array = [row.technology_category for row in top_technologies.collect()]
    lang_array = [row.primary_language for row in top_languages.collect()]
    
    # Calculate portfolio diversity score
    total_repos = portfolio_silver_df.count()
    tech_diversity = top_technologies.count() / max(1, total_repos)
    lang_diversity = top_languages.count() / max(1, total_repos)
    portfolio_diversity = (tech_diversity + lang_diversity) / 2.0
    
    # Determine activity level
    active_percentage = portfolio_overview_df.select(
        (F.col("active_repositories") / F.col("total_repositories")).alias("active_ratio")
    ).collect()[0].active_ratio
    
    activity_level = "high" if active_percentage >= 0.7 else "medium" if active_percentage >= 0.3 else "low"
    
    # Create final portfolio overview
    portfolio_overview_final_df = portfolio_overview_df.withColumn(
        "measurement_date", F.lit(PROCESSING_DATE)
    ).withColumn(
        "primary_technologies", F.array(*[F.lit(tech) for tech in tech_array])
    ).withColumn(
        "primary_languages", F.array(*[F.lit(lang) for lang in lang_array])
    ).withColumn(
        "portfolio_diversity_score", F.lit(portfolio_diversity)
    ).withColumn(
        "activity_level", F.lit(activity_level)
    ).withColumn(
        "partition_date", F.lit(PROCESSING_DATE)
    )
    
    print("✅ Portfolio overview generated")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Repository Health Dashboard Data

# COMMAND ----------
if portfolio_silver_df is not None:
    print("Generating repository health dashboard data...")
    
    # Get latest activity metrics for each repository (30d period)
    if activity_silver_df is not None:
        latest_activity = activity_silver_df.filter(F.col("measurement_period") == "30d")
        
        # Join portfolio with activity metrics
        repo_health_df = portfolio_silver_df.join(
            latest_activity.select(
                "repository_id",
                "total_commits",
                "total_issues", 
                "development_velocity",
                "last_activity_date"
            ),
            "repository_id",
            "left"
        )
    else:
        # Use portfolio data only
        repo_health_df = portfolio_silver_df.withColumn(
            "total_commits", F.lit(0)
        ).withColumn(
            "total_issues", F.lit(0)
        ).withColumn(
            "development_velocity", F.lit(0.0)
        ).withColumn(
            "last_activity_date", F.col("processed_timestamp")
        )
    
    # Calculate health scores and grades
    repo_health_final_df = repo_health_df.withColumn(
        "star_growth_30d", F.lit(0)  # Would need historical tracking
    ).withColumn(
        "commits_30d", F.coalesce(F.col("total_commits"), F.lit(0))
    ).withColumn(
        "issues_30d", F.coalesce(F.col("total_issues"), F.lit(0))
    ).withColumn(
        "health_score",
        F.least(F.lit(1.0),
                          (F.col("quality_score") * 0.4 +
               F.coalesce(F.col("development_velocity"), F.lit(0.0)) * 0.3 +
               F.when(F.col("is_active"), 0.3).otherwise(0.0)).cast(DoubleType())
        )
    ).withColumn(
        "health_grade",
        F.when(F.col("health_score") >= 0.8, "A")
         .when(F.col("health_score") >= 0.6, "B")
         .when(F.col("health_score") >= 0.4, "C")
         .when(F.col("health_score") >= 0.2, "D")
         .otherwise("F")
    ).withColumn(
        "activity_status",
        F.when(F.col("days_since_last_push") <= 7, "active")
         .when(F.col("days_since_last_push") <= 30, "stable")
         .otherwise("dormant")
    ).withColumn(
        "attention_needed",
        F.when((F.col("health_grade").isin(["D", "F"])) | 
               (F.col("activity_status") == "dormant") |
               (F.col("open_issues_count") > 10), True)
         .otherwise(False)
    ).withColumn(
        "recommended_actions",
        F.when(F.col("activity_status") == "dormant", 
               F.array(F.lit("review-purpose"), F.lit("archive-or-update")))
         .when(F.col("open_issues_count") > 10,
               F.array(F.lit("address-issues"), F.lit("triage-backlog")))
         .when(F.col("quality_score") < 0.5,
               F.array(F.lit("improve-documentation"), F.lit("add-license")))
         .otherwise(F.array(F.lit("maintain-current-status")))
    ).withColumn(
        "measurement_date", F.lit(PROCESSING_DATE)
    ).withColumn(
        "partition_date", F.lit(PROCESSING_DATE)
    )
    
    # Select final columns
    repo_health_dashboard_df = repo_health_final_df.select(
        "repository_id",
        F.col("name").alias("repository_name"),
        "technology_category",
        "stargazers_count",
        "star_growth_30d",
        "commits_30d",
        "issues_30d",
        "development_velocity",
        "health_grade",
        "health_score",
        "activity_status",
        "attention_needed",
        "recommended_actions",
        "last_activity_date",
        "measurement_date",
        "partition_date"
    )
    
    print(f"✅ Repository health dashboard data generated: {repo_health_dashboard_df.count()} repositories")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Development Velocity Trends

# COMMAND ----------
if activity_silver_df is not None:
    print("Generating development velocity trends...")
    
    # Create daily velocity data (simplified using current data)
    velocity_trends_df = activity_silver_df.filter(
        F.col("measurement_period") == "30d"
    ).select(
        F.lit(PROCESSING_DATE).alias("date"),
        "repository_id",
        F.col("repository_id").alias("repository_name"),  # Would join with repo names
        F.lit("unknown").alias("technology_category"),    # Would join with classifications
        F.col("total_commits").alias("daily_commits"),
        F.col("lines_added").alias("daily_lines_changed"),
        F.col("files_changed").alias("daily_files_changed"),
        F.col("total_commits").alias("cumulative_commits"),
        "development_velocity",
        "activity_trend"
    ).withColumn(
        "velocity_score", F.col("development_velocity")
    ).withColumn(
        "productivity_trend",
        F.when(F.col("activity_trend") == "increasing", "accelerating")
         .when(F.col("activity_trend") == "stable", "stable")
         .otherwise("slowing")
    ).withColumn(
        "partition_date", F.lit(PROCESSING_DATE)
    )
    
    print(f"✅ Development velocity trends generated: {velocity_trends_df.count()} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save All Analytics to Gold Layer

# COMMAND ----------
# Save Portfolio Overview
if 'portfolio_overview_final_df' in locals():
    print("Saving portfolio overview to Gold layer...")
    
    try:
        # Check if table exists
        table_exists = True
        try:
            existing_overview = spark.table("portfolio_overview")
            existing_count = existing_overview.count()
            print(f"Found existing portfolio overview table with {existing_count} records")
        except Exception as e:
            table_exists = False
            print("Portfolio overview table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        overview_records = portfolio_overview_final_df.count()
        
        if table_exists and overview_records > 0:
            print("Performing Portfolio Overview Delta merge...")
            
            portfolio_overview_final_df.createOrReplaceTempView("new_portfolio_overview_data")
            
            # SQL MERGE: Update portfolio overview
            merge_sql = """
            MERGE INTO portfolio_overview AS target
            USING new_portfolio_overview_data AS source
            ON target.measurement_date = source.measurement_date
            
            WHEN MATCHED THEN
              UPDATE SET *
              
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            print("✅ Portfolio overview merged successfully")
            
        else:
            # First time - create new table
            if overview_records > 0:
                (portfolio_overview_final_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date")
                 .saveAsTable("portfolio_overview"))
                print(f"✅ Created new portfolio overview table with {overview_records} records")
            else:
                print("⚠️ No portfolio overview data to save")
        
    except Exception as e:
        print(f"❌ Error with Portfolio Overview Delta merge: {e}")
        raise

# Save Repository Health Dashboard
if 'repo_health_dashboard_df' in locals():
    print("Saving repository health dashboard to Gold layer...")
    
    try:
        # Check if table exists
        table_exists = True
        try:
            existing_health = spark.table("repo_health_dashboard")
            existing_count = existing_health.count()
            print(f"Found existing repo health table with {existing_count} records")
        except Exception as e:
            table_exists = False
            print("Repo health table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        health_records = repo_health_dashboard_df.count()
        
        if table_exists and health_records > 0:
            print("Performing Repository Health Delta merge...")
            
            repo_health_dashboard_df.createOrReplaceTempView("new_repo_health_data")
            
            # SQL MERGE: Update repository health
            merge_sql = """
            MERGE INTO repo_health_dashboard AS target
            USING new_repo_health_data AS source
            ON target.repository_id = source.repository_id AND target.measurement_date = source.measurement_date
            
            WHEN MATCHED THEN
              UPDATE SET *
              
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            print("✅ Repository health dashboard merged successfully")
            
        else:
            # First time - create new table
            if health_records > 0:
                (repo_health_dashboard_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date")
                 .saveAsTable("repo_health_dashboard"))
                print(f"✅ Created new repo health table with {health_records} records")
            else:
                print("⚠️ No repo health data to save")
        
    except Exception as e:
        print(f"❌ Error with Repository Health Delta merge: {e}")
        raise

# Save Development Velocity
if 'velocity_trends_df' in locals():
    print("Saving development velocity to Gold layer...")
    
    try:
        # Check if table exists
        table_exists = True
        try:
            existing_velocity = spark.table("development_velocity")
            existing_count = existing_velocity.count()
            print(f"Found existing velocity table with {existing_count} records")
        except Exception as e:
            table_exists = False
            print("Velocity table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        velocity_records = velocity_trends_df.count()
        
        if table_exists and velocity_records > 0:
            print("Performing Development Velocity Delta merge...")
            
            velocity_trends_df.createOrReplaceTempView("new_velocity_data")
            
            # SQL MERGE: Update velocity data
            merge_sql = """
            MERGE INTO development_velocity AS target
            USING new_velocity_data AS source
            ON target.repository_id = source.repository_id AND target.date = source.date
            
            WHEN MATCHED THEN
              UPDATE SET *
              
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            print("✅ Development velocity merged successfully")
            
        else:
            # First time - create new table
            if velocity_records > 0:
                (velocity_trends_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date")
                 .saveAsTable("development_velocity"))
                print(f"✅ Created new velocity table with {velocity_records} records")
            else:
                print("No velocity data to save")
        
    except Exception as e:
        print(f"❌ Error with Development Velocity Delta merge: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Analytics Summary

# COMMAND ----------
print("Personal Repository Silver to Gold Analytics Complete!")
print("Processing Date: {}".format(PROCESSING_DATE))
print("\nAnalytics Generated:")

if 'portfolio_overview_final_df' in locals():
    overview_stats = portfolio_overview_final_df.collect()[0]
    total_repos = overview_stats.total_repositories
    active_repos = overview_stats.active_repositories
    total_stars = overview_stats.total_stars
    
    print("   Portfolio Overview:")
    print(f"      - Total repositories: {total_repos}")
    print(f"      - Active repositories: {active_repos}")
    print(f"      - Total stars: {total_stars}")
    print(f"      - Activity level: {overview_stats.activity_level}")

if 'repo_health_dashboard_df' in locals():
    health_stats = repo_health_dashboard_df.agg(
        F.count("repository_id").alias("total"),
        F.sum(F.when(F.col("attention_needed"), 1).otherwise(0)).alias("needs_attention")
    ).collect()[0]
    
    print("   Repository Health:")
    print(f"      - Repositories analyzed: {health_stats.total}")
    print(f"      - Repositories needing attention: {health_stats.needs_attention}")

if 'velocity_trends_df' in locals():
    velocity_stats = velocity_trends_df.agg(
        F.avg("velocity_score").alias("avg_velocity"),
        F.sum("daily_commits").alias("total_commits")
    ).collect()[0]
    
    print("   Development Velocity:")
    print(f"      - Average velocity score: {velocity_stats.avg_velocity:.2f}")
    print(f"      - Total recent commits: {velocity_stats.total_commits}")


print("\n✅ Analytics ready for Power BI dashboards!")

# COMMAND ----------
# Clean up
spark.stop()
