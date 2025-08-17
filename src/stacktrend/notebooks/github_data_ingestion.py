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
        current_year = datetime.now().year
        last_year = current_year - 1
        
        # Comprehensive search queries - ALL repos must have >1000 stars
        search_queries = [
            # High-impact AI & ML (>1000 stars)
            "artificial-intelligence+machine-learning+stars:>1000+pushed:>2023-01-01",
            "deep-learning+neural-networks+pytorch+tensorflow+stars:>1000",
            "large-language-model+LLM+GPT+transformer+stars:>1000",
            "computer-vision+opencv+image-processing+stars:>1000",
            "natural-language-processing+NLP+huggingface+stars:>1000",
            
            # Generative AI & Agent Systems (>1000 stars)
            "generative-ai+openai+chatgpt+stable-diffusion+stars:>1000",
            "langchain+llamaindex+ai-agent+autonomous+stars:>1000", 
            "retrieval-augmented-generation+RAG+vector-database+stars:>1000",
            "prompt-engineering+fine-tuning+model-training+stars:>1000",
            
            # Data Engineering & Analytics (>1000 stars)
            "data-engineering+ETL+data-pipeline+apache-spark+stars:>1000",
            "apache-airflow+workflow+orchestration+data-processing+stars:>1000",
            "dbt+analytics-engineering+data-transformation+stars:>1000",
            "real-time+streaming+apache-kafka+event-driven+stars:>1000",
            "big-data+distributed-computing+hadoop+elasticsearch+stars:>1000",
            
            # Databases & Infrastructure (>1000 stars)
            "database+SQL+NoSQL+postgresql+mongodb+stars:>1000",
            "vector-database+embeddings+similarity-search+stars:>1000",
            "redis+caching+in-memory+key-value+stars:>1000",
            "time-series+metrics+monitoring+prometheus+stars:>1000",
            
            # Web Development & Frameworks (>1000 stars)
            "web-development+framework+react+vue+angular+stars:>1000",
            "backend+API+microservices+node+python+stars:>1000",
            "frontend+javascript+typescript+css+stars:>1000",
            "fullstack+web-application+mobile-app+stars:>1000",
            
            # DevOps & Cloud Native (>1000 stars)
            "devops+kubernetes+docker+containerization+stars:>1000",
            "ci-cd+github-actions+automation+deployment+stars:>1000",
            "infrastructure+terraform+cloud+AWS+azure+stars:>1000",
            "monitoring+observability+logging+grafana+stars:>1000",
            "security+cybersecurity+authentication+encryption+stars:>1000",
            
            # Programming Languages & Tools (>1000 stars)
            "python+golang+rust+java+cpp+stars:>1000",
            "javascript+typescript+node+deno+stars:>1000", 
            "developer-tools+CLI+productivity+automation+stars:>1000",
            "open-source+library+framework+SDK+stars:>1000"
        ]
        
        all_repositories = []
        successful_queries = 0
        
        # Get multiple pages for each query to maximize data
        for i, query in enumerate(search_queries[:12]):  # Use more queries for >1000 star repos
            for page in range(1, 6):  # Get 5 pages per query (500 repos per query)
                try:
                    url = f"https://api.github.com/search/repositories?q={query}&sort=updated&order=desc&per_page=100&page={page}"
                    response = requests.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        repositories = data.get("items", [])
                        all_repositories.extend(repositories)
                        successful_queries += 1
                        print(f"Query {i+1} Page {page}: {len(repositories)} repos - {query[:50]}...")
                    else:
                        print(f"Query {i+1} Page {page} failed: {response.status_code}")
                        
                except Exception as e:
                    print(f"Query {i+1} Page {page} error: {e}")
                    continue
        
        # Deduplicate repositories by ID and filter for >1000 stars
        seen_repos = set()
        filtered_repositories = []
        
        for repo in all_repositories:
            repo_id = repo.get('id')
            stars = repo.get('stargazers_count', 0)
            
            # Only include repos with >1000 stars and not already seen
            if repo_id not in seen_repos and stars > 1000:
                seen_repos.add(repo_id)
                filtered_repositories.append(repo)
        
        # Combine all data
        combined_data = {"items": filtered_repositories}
        github_response = json.dumps(combined_data)
        
        print(f"Retrieved {len(all_repositories)} total repositories from {successful_queries} queries")
        print(f"After deduplication and >1000 star filter: {len(filtered_repositories)} repositories")
        
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
# Write to Bronze lakehouse (using default lakehouse configuration)
try:
    if record_count > 0:
        bronze_df.write.format("delta").mode("overwrite").saveAsTable("github_repositories")
        print(f"SUCCESS: Saved {record_count} records to Bronze lakehouse")
    else:
        print("WARNING: No data to save - creating empty table with proper schema")
        # Save the empty DataFrame with schema to ensure table exists
        bronze_df.write.format("delta").mode("overwrite").saveAsTable("github_repositories")
        print("SUCCESS: Created empty Bronze table with proper schema")
        
        # Log the issue for debugging
        print("DEBUGGING: No repositories were ingested")
        print("Possible causes:")
        print("1. GitHub API parameter not passed from Data Factory")
        print("2. GitHub API calls failed (rate limits, network issues)")
        print("3. Empty API responses")
    
except Exception as e:
    print(f"ERROR: Error saving to Bronze lakehouse: {e}")
    print(f"Record count: {record_count}")
    print("DataFrame schema:")
    bronze_df.printSchema()
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"Ingestion completed: {record_count} records on {PROCESSING_DATE}")