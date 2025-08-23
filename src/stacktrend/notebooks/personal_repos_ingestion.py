# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_bronze_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
# MAGIC %md
# MAGIC # Personal GitHub Repository Monitoring - Data Ingestion
# MAGIC 
# MAGIC Microsoft Fabric Notebook for ingesting personal GitHub repository data into Bronze lakehouse
# MAGIC 
# MAGIC This notebook collects repository and activity data from your GitHub account for monitoring and dashboards.

# COMMAND ----------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    BooleanType, ArrayType
)
from datetime import datetime, timedelta
import requests
import time

# Initialize Spark Session
spark = SparkSession.builder.appName("Personal_GitHub_Repos_Ingestion").getOrCreate()

# COMMAND ----------
# Configuration
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
GITHUB_API_BASE = "https://api.github.com"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Get Pipeline Parameters

# COMMAND ----------
# PARAMETERS CELL: Define parameters that Data Factory will pass
# This cell must be marked as "parameter cell" in Fabric (click ... → Toggle parameter cell)
# Only set if not already defined (prevents overwriting actual values from Data Factory)
if 'github_token' not in locals() or not github_token:  # noqa: F821
    github_token = ""
if 'github_username' not in locals() or not github_username:  # noqa: F821
    github_username = ""
if 'include_private' not in locals() or not include_private:  # noqa: F821
    include_private = False

# COMMAND ----------
# Validate parameters
print("Checking for Data Factory pipeline parameters...")

if not github_token or github_token == "":
    raise ValueError("GitHub token is required for API access")

if not github_username or github_username == "":
    raise ValueError("GitHub username is required for personal repo monitoring")

# Set up headers for GitHub API
headers = {
    "Authorization": f"token {github_token}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "stacktrend-personal-monitor"
}

print("✅ GitHub API credentials configured")
print(f"   Monitoring user: {github_username}")
print(f"   Include private repos: {include_private}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Collect Personal Repository Data

# COMMAND ----------
def get_user_repositories(username, include_private=False):
    """Get all repositories for a specific user."""
    repos_url = f"{GITHUB_API_BASE}/users/{username}/repos"
    all_repositories = []
    page = 1
    
    while True:
        params = {
            "per_page": 100,
            "page": page,
            "type": "all" if include_private else "public",
            "sort": "updated",
            "direction": "desc"
        }
        
        try:
            response = requests.get(repos_url, headers=headers, params=params)
            
            if response.status_code == 200:
                repositories = response.json()
                
                if not repositories:  # No more repositories
                    break
                    
                # Add metadata to each repository
                for repo in repositories:
                    repo['ingestion_timestamp'] = datetime.now().isoformat()
                    repo['partition_date'] = PROCESSING_DATE
                
                all_repositories.extend(repositories)
                page += 1
                
                # Rate limiting
                time.sleep(0.2)
                
            elif response.status_code == 403:
                print("Rate limited, waiting...")
                time.sleep(60)
                continue
                
            else:
                print("Error getting repositories: {}".format(response.status_code))
                break
                
        except Exception as e:
            print(f"Exception getting repositories: {e}")
            break
    
    return all_repositories

# Collect personal repositories
print(f"Collecting repositories for user: {github_username}")
personal_repositories = get_user_repositories(github_username, include_private)

print(f"✅ Collected {len(personal_repositories)} repositories")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Collect Repository Activity Data

# COMMAND ----------
def get_repository_activity(repo_full_name, repo_id):
    """Get recent activity (commits, issues) for a repository."""
    activity_data = []
    
    # Get recent commits (last 30 days)
    commits_url = f"{GITHUB_API_BASE}/repos/{repo_full_name}/commits"
    since_date = (datetime.now() - timedelta(days=30)).isoformat()
    
    try:
        response = requests.get(commits_url, headers=headers, params={
            "since": since_date,
            "per_page": 50
        })
        
        if response.status_code == 200:
            commits = response.json()
            
            for commit in commits:
                activity_data.append({
                    'repository_id': repo_id,
                    'activity_type': 'commit',
                    'activity_id': commit['sha'],
                    'author_login': commit.get('author', {}).get('login') if commit.get('author') else None,
                    'activity_date': commit['commit']['author']['date'],
                    'title': commit['commit']['message'][:200],
                    'additions': None,  # Would need detailed commit API
                    'deletions': None,
                    'changed_files': None,
                    'state': None,
                    'ingestion_timestamp': datetime.now().isoformat(),
                    'partition_date': PROCESSING_DATE
                })
        
        time.sleep(0.5)  # Rate limiting
        
    except Exception as e:
                    print("Error getting commits for {}: {}".format(repo_full_name, e))
    
    # Get recent issues (last 30 days)
    issues_url = f"{GITHUB_API_BASE}/repos/{repo_full_name}/issues"
    
    try:
        response = requests.get(issues_url, headers=headers, params={
            "state": "all",
            "since": since_date,
            "per_page": 30
        })
        
        if response.status_code == 200:
            issues = response.json()
            
            for issue in issues:
                # Skip pull requests (they appear in issues API)
                if 'pull_request' not in issue:
                    activity_data.append({
                        'repository_id': repo_id,
                        'activity_type': 'issue',
                        'activity_id': str(issue['id']),
                        'author_login': issue.get('user', {}).get('login'),
                        'activity_date': issue['created_at'],
                        'title': issue['title'][:200],
                        'additions': None,
                        'deletions': None,
                        'changed_files': None,
                        'state': issue['state'],
                        'ingestion_timestamp': datetime.now().isoformat(),
                        'partition_date': PROCESSING_DATE
                    })
        
        time.sleep(0.5)  # Rate limiting
        
    except Exception as e:
                    print("Error getting issues for {}: {}".format(repo_full_name, e))
    
    return activity_data

# Collect activity data for personal repositories
print("Collecting activity data for repositories...")
all_activity_data = []

# Process top 20 most recently updated repositories for activity
recent_repos = sorted(personal_repositories, key=lambda x: x.get('updated_at', ''), reverse=True)[:20]

for i, repo in enumerate(recent_repos):
    repo_id = repo['id']
    repo_full_name = repo['full_name']
    
    print(f"Processing activity {i+1}/20: {repo_full_name}")
    
    try:
        activity = get_repository_activity(repo_full_name, repo_id)
        all_activity_data.extend(activity)
        print(f"  → Collected {len(activity)} activities")
        
    except Exception as e:
        print(f"  ❌ Error processing {repo_full_name}: {e}")
        continue

print(f"✅ Total activities collected: {len(all_activity_data)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Repository Data to Bronze Layer

# COMMAND ----------
# Define schema for repository data
repos_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("private", BooleanType(), True),
    StructField("language", StringType(), True),
    StructField("stargazers_count", LongType(), True),
    StructField("forks_count", LongType(), True),
    StructField("open_issues_count", LongType(), True),
    StructField("size", LongType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("pushed_at", StringType(), True),
    StructField("topics", ArrayType(StringType()), True),
    StructField("license", StructType([
        StructField("name", StringType(), True)
    ]), True),
    StructField("default_branch", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("partition_date", StringType(), True)
])

# Save repositories data
if personal_repositories:
    print("Saving personal repositories to Bronze layer...")
    
    # Create DataFrame
    repos_df = spark.createDataFrame(personal_repositories, repos_schema)
    
    # Clean up data types and add derived columns
    repos_df = repos_df.withColumn(
        "repository_id", F.col("id")
    ).withColumn(
        "license_name", F.col("license.name")
    ).withColumn(
        "created_at", F.to_timestamp(F.col("created_at"))
    ).withColumn(
        "updated_at", F.to_timestamp(F.col("updated_at"))
    ).withColumn(
        "pushed_at", F.to_timestamp(F.col("pushed_at"))
    ).withColumn(
        "ingestion_timestamp", F.to_timestamp(F.col("ingestion_timestamp"))
    ).drop("id", "license")
    
    # Smart Delta merge to Bronze lakehouse using SQL MERGE pattern
    try:
        # Check if table exists for merge vs create
        table_exists = True
        try:
            existing_df = spark.table("github_my_repos")
            existing_count = existing_df.count()
            print(f"Found existing personal repos table with {existing_count} records")
        except Exception as e:
            table_exists = False
            print("Personal repos table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        if table_exists:
            print("Performing Personal Repos Delta merge upsert...")
            
            # Create temporary view for merge
            repos_df.createOrReplaceTempView("new_personal_repos_data")
            
            # SQL MERGE: Update metrics for existing repos, insert new ones
            merge_sql = """
            MERGE INTO github_my_repos AS target
            USING new_personal_repos_data AS source
            ON target.repository_id = source.repository_id
            
            WHEN MATCHED THEN
              UPDATE SET
                name = source.name,
                full_name = source.full_name,
                description = source.description,
                language = source.language,
                stargazers_count = source.stargazers_count,
                forks_count = source.forks_count,
                open_issues_count = source.open_issues_count,
                size = source.size,
                updated_at = source.updated_at,
                pushed_at = source.pushed_at,
                topics = source.topics,
                license_name = source.license_name,
                default_branch = source.default_branch,
                ingestion_timestamp = source.ingestion_timestamp,
                partition_date = source.partition_date
                
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            
            # Get merge statistics
            final_count = spark.table("github_my_repos").count()
            new_records = final_count - existing_count
            updated_records = len(personal_repositories) - max(0, new_records)
            
            print("Personal Repos Delta merge completed:")
            print(f"  Total records now: {final_count}")
            print(f"  New records added: {max(0, new_records)}")
            print(f"  Records updated: {updated_records}")
            
        else:
            # First time - create new table
            (repos_df
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .partitionBy("partition_date")
             .saveAsTable("github_my_repos"))
            print(f"✅ Created new personal repos table with {len(personal_repositories)} records")
            
    except Exception as e:
        print(f"❌ Error with Personal Repos Delta merge: {e}")
        raise
    
    print("Personal Repositories Statistics:")
    print("   Total repositories: {}".format(len(personal_repositories)))
    
else:
    print("⚠️  No repository data to save")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Activity Data to Bronze Layer

# COMMAND ----------
# Define schema for activity data
activity_schema = StructType([
    StructField("repository_id", LongType(), True),
    StructField("activity_type", StringType(), True),
    StructField("activity_id", StringType(), True),
    StructField("author_login", StringType(), True),
    StructField("activity_date", StringType(), True),
    StructField("title", StringType(), True),
    StructField("additions", LongType(), True),
    StructField("deletions", LongType(), True),
    StructField("changed_files", LongType(), True),
    StructField("state", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("partition_date", StringType(), True)
])

# Save activity data
if all_activity_data:
    print("Saving repository activity to Bronze layer...")
    
    # Create DataFrame
    activity_df = spark.createDataFrame(all_activity_data, activity_schema)
    
    # Convert timestamps
    activity_df = activity_df.withColumn(
        "activity_date", F.to_timestamp(F.col("activity_date"))
    ).withColumn(
        "ingestion_timestamp", F.to_timestamp(F.col("ingestion_timestamp"))
    )
    
    # Smart Delta merge to Bronze lakehouse using SQL MERGE pattern
    try:
        # Check if table exists for merge vs create
        table_exists = True
        try:
            existing_df = spark.table("github_repo_activity")
            existing_count = existing_df.count()
            print(f"Found existing activity table with {existing_count} records")
        except Exception as e:
            table_exists = False
            print("Activity table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        if table_exists:
            print("Performing Activity Delta merge upsert...")
            
            # Create temporary view for merge
            activity_df.createOrReplaceTempView("new_activity_data")
            
            # SQL MERGE: Insert new activities only (activities are immutable)
            merge_sql = """
            MERGE INTO github_repo_activity AS target
            USING new_activity_data AS source
            ON target.activity_id = source.activity_id AND target.activity_type = source.activity_type
            
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            
            # Get merge statistics
            final_count = spark.table("github_repo_activity").count()
            new_records = final_count - existing_count
            
            print("Activity Delta merge completed:")
            print(f"  Total records now: {final_count}")
            print(f"  New records added: {new_records}")
            
        else:
            # First time - create new table
            (activity_df
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .partitionBy("partition_date", "activity_type")
             .saveAsTable("github_repo_activity"))
            print(f"✅ Created new activity table with {len(all_activity_data)} records")
            
    except Exception as e:
        print(f"❌ Error with Activity Delta merge: {e}")
        raise
    
    print("Activity Statistics:")
    print("   Total activities: {}".format(len(all_activity_data)))
    
else:
    print("⚠️  No activity data to save")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("Personal GitHub Repository Data Ingestion Complete!")
print("Processing Date: {}".format(PROCESSING_DATE))
print("GitHub User: {}".format(github_username))
print("\nSummary:")
print("   Repositories collected: {}".format(len(personal_repositories)))
print("   Activities collected: {}".format(len(all_activity_data)))


print("\nData ready for Silver layer processing!")

# COMMAND ----------
# Clean up
spark.stop()
