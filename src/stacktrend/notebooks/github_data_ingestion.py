# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_bronze_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
# MAGIC %md
# MAGIC # GitHub Data Ingestion Notebook
# MAGIC 
# MAGIC Microsoft Fabric Notebook for ingesting GitHub API data into Bronze lakehouse
# MAGIC 
# MAGIC This notebook takes GitHub API JSON response and saves it to Bronze lakehouse
# MAGIC in Delta format for further processing.
# MAGIC 
# MAGIC ## Usage in Fabric Data Factory:
# MAGIC 1. Connect as Notebook Activity after Web Activity
# MAGIC 2. Pass Web Activity output as parameter: @activity('get_repo').output
# MAGIC 3. Notebook saves data to Bronze lakehouse

# COMMAND ----------
# MAGIC %md
# MAGIC # GitHub Data Ingestion to Bronze Layer
# MAGIC 
# MAGIC This notebook ingests GitHub API response data and saves it to the Bronze lakehouse in Delta format.

# COMMAND ----------
# Import required libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    TimestampType, BooleanType, ArrayType
)
from datetime import datetime
import json
import requests
import random


# Initialize Spark Session
spark = SparkSession.builder.appName("GitHub_Data_Ingestion").getOrCreate()

# COMMAND ----------
# Configuration
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Get Pipeline Parameters
# MAGIC 
# MAGIC In Fabric Data Factory, the Web Activity output will be passed as a notebook parameter

# COMMAND ----------
# PARAMETERS CELL: Define parameters that Data Factory will pass
# This cell must be marked as "parameter cell" in Fabric (click ... → Toggle parameter cell)
github_api_response = ""

# COMMAND ----------
# Get the GitHub API response from Data Factory parameter
print("DEBUGGING: Checking for Data Factory pipeline parameters...")

# Validate parameter from Data Factory
if github_api_response and str(github_api_response).strip():
    # Check if it's not just empty/null values
    if str(github_api_response).strip() not in ["null", "None", "{}", "[]", ""]:
        github_response = github_api_response
        print("SUCCESS: Found 'github_api_response' parameter from Data Factory")
        print(f"Parameter type: {type(github_response)}")
        print(f"Parameter length: {len(str(github_response))} characters")
        print(f"SUCCESS: Valid parameter received: {str(github_response)[:200]}...")
    else:
        print("WARNING: Parameter contains null/empty values")
        github_response = None
else:
    print("ERROR: 'github_api_response' parameter not found or empty")
    github_response = None

# Final parameter validation
if github_response:
    print(f"SUCCESS: Using pipeline parameter ({len(str(github_response))} characters)")
else:
    print("ERROR: No valid pipeline parameter found - falling back to direct GitHub API call")
    print("This usually means:")
    print("1. Data Factory Web Activity failed")
    print("2. Parameter name mismatch in Data Factory pipeline")
    print("3. Parameter value is empty/null")
    try:
        # Smart GitHub data collection with randomization
        def get_existing_repository_ids():
            """Get existing repository IDs to balance old/new repos."""
            try:
                existing_df = spark.table("github_repos")
                existing_ids = [row.repository_id for row in existing_df.select("repository_id").distinct().collect()]
                print(f"Found {len(existing_ids)} existing repositories")
                return set(existing_ids)
            except Exception:
                print("No existing repositories found - first run")
                return set()
        
        def create_randomized_search_queries():
            """Create randomized search queries for balanced old/new repo discovery."""
            base_categories = [
                "artificial-intelligence+machine-learning",
                "deep-learning+neural-networks+pytorch+tensorflow", 
                "large-language-model+LLM+GPT+transformer",
                "data-engineering+ETL+data-pipeline+apache-spark",
                "web-development+framework+react+vue+angular",
                "devops+kubernetes+docker+containerization",
                "database+SQL+NoSQL+postgresql+mongodb",
                "security+cybersecurity+authentication",
                "python+golang+rust+java+cpp",
                "javascript+typescript+node+frontend"
            ]
            
            # Randomized sorting for variety
            sort_strategies = [
                ("stars", "desc"),     # Popular repos
                ("updated", "desc"),   # Recently updated  
                ("created", "desc"),   # New repos
                ("forks", "desc"),     # Most forked
                ("help-wanted-issues", "desc")  # Community-driven
            ]
            
            # Different time filters for repo age variety
            time_filters = [
                "pushed:>2024-06-01",   # Very recent
                "pushed:>2024-01-01",   # This year
                "pushed:>2023-01-01",   # Last year
                "created:>2024-01-01",  # New repos
                "created:>2023-01-01"   # Newer repos
            ]
            
            queries = []
            
            # Generate randomized combinations
            for _ in range(12):  # 12 randomized queries
                category = random.choice(base_categories)
                sort_field, sort_order = random.choice(sort_strategies)
                time_filter = random.choice(time_filters)
                star_min = random.choice([1000, 1500, 2000, 5000])
                
                query = f"{category}+stars:>{star_min}+{time_filter}"
                queries.append((query, sort_field, sort_order))
            
            # Always include some guaranteed high-quality queries
            guaranteed = [
                ("stars:>10000+created:>2023-01-01", "stars", "desc"),
                ("stars:>5000+updated:>2024-06-01", "updated", "desc") 
            ]
            queries.extend(guaranteed)
            
            return queries
        
        existing_repo_ids = get_existing_repository_ids()
        search_queries = create_randomized_search_queries()
        
        all_repositories = []
        new_repositories = []
        updated_repositories = []
        successful_queries = 0
        
        print(f"Starting smart collection with {len(search_queries)} randomized queries...")
        
        # Smart collection with randomization
        for i, (query, sort_field, sort_order) in enumerate(search_queries):
            # Randomize page selection for variety
            pages_to_fetch = random.sample(range(1, 6), 3)  # Random 3 pages from first 5
            
            for page in pages_to_fetch:
                try:
                    url = f"https://api.github.com/search/repositories?q={query}&sort={sort_field}&order={sort_order}&per_page=100&page={page}"
                    response = requests.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        repositories = data.get("items", [])
                        
                        for repo in repositories:
                            repo_id = repo.get('id')
                            if repo_id in existing_repo_ids:
                                updated_repositories.append(repo)
                            else:
                                new_repositories.append(repo)
                        
                        all_repositories.extend(repositories)
                        successful_queries += 1
                        print(f"Query {i+1} Page {page}: {len(repositories)} repos - Sort: {sort_field}")
                    else:
                        print(f"Query {i+1} Page {page} failed: {response.status_code}")
                        
                except Exception as e:
                    print(f"Query {i+1} Page {page} error: {e}")
                    continue
        
        # Smart deduplication with old/new repo tracking
        seen_repos = set()
        filtered_repositories = []
        new_repo_count = 0
        updated_repo_count = 0
        
        for repo in all_repositories:
            repo_id = repo.get('id')
            stars = repo.get('stargazers_count', 0)
            
            # Only include repos with >1000 stars and not already seen in this batch
            if repo_id not in seen_repos and stars > 1000:
                seen_repos.add(repo_id)
                filtered_repositories.append(repo)
                
                # Track new vs updated repos
                if repo_id in existing_repo_ids:
                    updated_repo_count += 1
                else:
                    new_repo_count += 1
        
        # Combine all data
        combined_data = {"items": filtered_repositories}
        github_response = json.dumps(combined_data)
        
        print("Smart Collection Results:")
        print(f"  Total retrieved: {len(all_repositories)} from {successful_queries} queries")
        print(f"  After deduplication: {len(filtered_repositories)} repositories")
        print(f"  New repositories: {new_repo_count}")
        print(f"  Updated repositories: {updated_repo_count}")
        print(f"  Balance ratio: {new_repo_count}/{updated_repo_count} (new/updated)")
        
    except Exception as e:
        raise Exception(f"Could not get GitHub data: {e}")

print("Proceeding with GitHub data processing...")
print("CI/CD Pipeline Test: Notebook updated successfully!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parse and Structure Data

# COMMAND ----------
# Parse the JSON response
try:
    if isinstance(github_response, str):
        api_data = json.loads(github_response)
    else:
        api_data = github_response
    
    repositories = api_data.get("items", [])
    print(f"Parsed {len(repositories)} repositories from API response")
    
    # Debug: Show structure of first repository
    if len(repositories) > 0:
        print("First repository keys:", list(repositories[0].keys()))
    
    if len(repositories) == 0:
        # Create empty DataFrame with proper schema for consistency
        schema = StructType([
            StructField("repository_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner_login", StringType(), True),
            StructField("owner_type", StringType(), True),
            StructField("description", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("pushed_at", TimestampType(), True),
            StructField("language", StringType(), True),
            StructField("stargazers_count", LongType(), True),
            StructField("watchers_count", LongType(), True),
            StructField("forks_count", LongType(), True),
            StructField("open_issues_count", LongType(), True),
            StructField("size", LongType(), True),
            StructField("default_branch", StringType(), True),
            StructField("topics", ArrayType(StringType()), True),
            StructField("license_name", StringType(), True),
            StructField("has_wiki", BooleanType(), True),
            StructField("has_pages", BooleanType(), True),
            StructField("archived", BooleanType(), True),
            StructField("disabled", BooleanType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("partition_date", StringType(), True)
        ])
        bronze_df = spark.createDataFrame([], schema)
    else:
        # Convert repositories to JSON strings for Spark
        repositories_json_strings = [json.dumps(repo) for repo in repositories]
        repositories_rdd = spark.sparkContext.parallelize(repositories_json_strings)
        raw_df = spark.read.json(repositories_rdd)
        
        print("DataFrame schema after JSON parsing:")
        raw_df.printSchema()
        
        # Structure the data for Bronze layer
        bronze_df = (raw_df
            .select(
                F.col("id").alias("repository_id"),
                F.col("name"),
                F.col("full_name"),
                F.col("owner.login").alias("owner_login"),
                F.col("owner.type").alias("owner_type"),
                F.col("description"),
                F.to_timestamp(F.col("created_at")).alias("created_at"),
                F.to_timestamp(F.col("updated_at")).alias("updated_at"),
                F.to_timestamp(F.col("pushed_at")).alias("pushed_at"),
                F.col("language"),
                F.col("stargazers_count"),
                F.col("watchers_count"),
                F.col("forks_count"),
                F.col("open_issues_count"),
                F.col("size"),
                F.col("default_branch"),
                F.col("topics"),
                F.col("license.name").alias("license_name"),
                F.col("has_wiki"),
                F.col("has_pages"),
                F.col("archived"),
                F.col("disabled")
            )
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("partition_date", F.lit(PROCESSING_DATE))
        )
    
    record_count = bronze_df.count()
    
except Exception as e:
    print(f"Error parsing GitHub API response: {e}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save to Bronze Lakehouse

# COMMAND ----------
# Smart Delta merge to Bronze lakehouse
try:
    if record_count > 0:
        # Check if table exists for merge vs create
        table_exists = True
        try:
            existing_df = spark.table("github_repos")
            existing_count = existing_df.count()
            print(f"Found existing table with {existing_count} records")
        except Exception:
            table_exists = False
            print("Table doesn't exist - will create new table")
        
        if table_exists:
            print("Performing Delta merge upsert...")
            
            # Create temporary view for merge
            bronze_df.createOrReplaceTempView("new_github_data")
            
            # DELTA MERGE: Update metrics for existing repos, insert new ones
            merge_sql = """
            MERGE INTO github_repos AS target
            USING new_github_data AS source
            ON target.repository_id = source.repository_id
            
            WHEN MATCHED THEN
              UPDATE SET
                name = source.name,
                full_name = source.full_name,
                description = source.description,
                updated_at = source.updated_at,
                pushed_at = source.pushed_at,
                stargazers_count = source.stargazers_count,
                watchers_count = source.watchers_count,
                forks_count = source.forks_count,
                open_issues_count = source.open_issues_count,
                size = source.size,
                topics = source.topics,
                has_wiki = source.has_wiki,
                has_pages = source.has_pages,
                archived = source.archived,
                disabled = source.disabled,
                ingestion_timestamp = source.ingestion_timestamp
                
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            
            # Get merge statistics
            final_count = spark.table("github_repos").count()
            new_records = final_count - existing_count
            updated_records = record_count - max(0, new_records)
            
            print("Delta merge completed:")
            print(f"  Total records now: {final_count}")
            print(f"  New records added: {max(0, new_records)}")
            print(f"  Records updated: {updated_records}")
            
        else:
            # First time - create table with overwrite
            bronze_df.write.format("delta").mode("overwrite").saveAsTable("github_repos")
            print(f"Created new Bronze table with {record_count} records")
            
    else:
        print("No data to save - creating/maintaining empty table")
        if not table_exists:
            bronze_df.write.format("delta").mode("overwrite").saveAsTable("github_repos")
            print("✅ Created empty Bronze table with proper schema")
        else:
            print("Existing table maintained, no new data to merge")
    
except Exception as e:
    print(f"❌ Error with Delta merge: {e}")
    print("Falling back to overwrite mode...")
    try:
        bronze_df.write.format("delta").mode("overwrite").saveAsTable("github_repos")
        print(f"✅ Fallback successful: Saved {record_count} records")
    except Exception as fallback_error:
        print(f"❌ Fallback also failed: {fallback_error}")
        print(f"Record count: {record_count}")
        bronze_df.printSchema()
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"Ingestion completed: {record_count} records on {PROCESSING_DATE}")