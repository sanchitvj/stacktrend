"""
GitHub Data Ingestion Notebook
Microsoft Fabric Notebook for ingesting GitHub API data into Bronze lakehouse

This notebook takes GitHub API JSON response and saves it to Bronze lakehouse
in Delta format for further processing.

Usage in Fabric Data Factory:
1. Connect as Notebook Activity after Web Activity
2. Pass Web Activity output as parameter: @activity('get_repo').output
3. Notebook saves data to Bronze lakehouse
"""

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
# Configuration - Use explicit lakehouse references (independent of attachments)
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Get Pipeline Parameters
# MAGIC 
# MAGIC In Fabric Data Factory, the Web Activity output will be passed as a notebook parameter

# COMMAND ----------
# Get the GitHub API response from pipeline parameter
try:
    from notebookutils import mssparkutils
    params = mssparkutils.notebook.getParameters()
    if 'github_api_response' in params:
        print("Found GitHub API parameter")
except Exception as e:
    print(f"Parameter check failed: {e}")

# Try to get GitHub API data from pipeline parameters
github_response = None
try:
    from notebookutils import mssparkutils
    params = mssparkutils.notebook.getParameters()
    
    if 'github_api_response' in params:
        github_response = params['github_api_response']
        print(f"Received GitHub API data: {len(str(github_response))} characters")
    else:
        # Try Spark configuration as fallback
        github_response = spark.conf.get('github_api_response')
        if github_response:
            print(f"Received GitHub API data via spark.conf: {len(str(github_response))} characters")
            
except Exception as e:
    print(f"Parameter retrieval failed: {e}")

# Final check
if github_response:
    print(f"SUCCESS: Received GitHub API data ({len(str(github_response))} characters)")
else:
    print("No parameter found, using direct GitHub API call")
    try:
        current_year = datetime.now().year
        last_year = current_year - 1
        
        # Smart search queries based on 2024 AI/ML and Data Engineering trends
        search_queries = [
            # Agentic AI & AI Agents
            "agentic-ai+ai-agents+autonomous+stars:>20+pushed:>2023-01-01",
            "langchain+autogen+crewai+multi-agent+stars:>50",
            "ai-assistant+chatbot+conversational-ai+stars:>30",
            "semantic-kernel+ai-orchestration+stars:>20",
            
            # Generative AI & LLMs
            "generative-ai+large-language-model+LLM+stars:>50",
            "ollama+local-llm+open-source-llm+stars:>30",
            "huggingface+transformers+fine-tuning+stars:>100",
            "prompt-engineering+prompt-optimization+stars:>20",
            
            # Retrieval & Knowledge Systems
            "retrieval-augmented-generation+RAG+vector-database+stars:>30",
            "embeddings+semantic-search+similarity+stars:>20",
            "llamaindex+vector-store+knowledge-base+stars:>50",
            "pinecone+chroma+weaviate+qdrant+stars:>20",
            
            # Computer Vision & Multimodal AI
            "computer-vision+object-detection+image-processing+stars:>30",
            "multimodal-ai+vision-language+CLIP+stars:>20",
            "stable-diffusion+image-generation+diffusion+stars:>50",
            "opencv+yolo+detectron+stars:>30",
            
            # Machine Learning & Deep Learning
            "pytorch+tensorflow+jax+deep-learning+stars:>100",
            "scikit-learn+xgboost+lightgbm+machine-learning+stars:>50",
            "reinforcement-learning+RL+gym+stable-baselines+stars:>20",
            "federated-learning+distributed-ml+stars:>10",
            
            # Data Engineering & MLOps
            "data-engineering+ETL+data-pipeline+stars:>30",
            "apache-airflow+prefect+dagster+workflow+stars:>50",
            "dbt+data-transformation+analytics-engineering+stars:>100",
            "mlops+model-deployment+mlflow+kubeflow+stars:>30",
            
            # Real-time & Streaming
            "apache-kafka+stream-processing+real-time+stars:>50",
            "apache-spark+big-data+distributed-computing+stars:>100",
            "flink+storm+streaming+event-driven+stars:>20",
            "redis+message-queue+pub-sub+stars:>30",
            
            # Data Infrastructure & Observability
            "data-observability+data-quality+monitoring+stars:>20",
            "airbyte+fivetran+data-integration+ELT+stars:>30",
            "snowflake+databricks+data-warehouse+lakehouse+stars:>20",
            "prometheus+grafana+observability+monitoring+stars:>50",
            
            # Cloud-Native & DevOps
            "kubernetes+cloud-native+microservices+stars:>100",
            "terraform+infrastructure-as-code+IaC+stars:>50",
            "docker+containerization+orchestration+stars:>100",
            "github-actions+ci-cd+automation+stars:>30"
        ]
        
        all_repositories = []
        successful_queries = 0
        
        # Get multiple pages for each query to maximize data
        for i, query in enumerate(search_queries[:6]):  # Limit to avoid rate limits
            for page in range(1, 4):  # Get 3 pages per query
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
        
        # Combine all data
        combined_data = {"items": all_repositories}
        github_response = json.dumps(combined_data)
        
        print(f"Retrieved {len(all_repositories)} repositories from {successful_queries} queries")
        
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
# Write to Bronze lakehouse in Delta format
try:
    # First, try to create the table directly
    # Use explicit lakehouse reference to avoid attachment dependency
    bronze_df.write.format("delta").mode("overwrite").option("path", "Tables/github_repositories").saveAsTable("stacktrend_bronze_lh.github_repositories")
    print(f"Saved {record_count} records to Bronze lakehouse")
    
except Exception as e:
    print(f"Error saving to Bronze lakehouse: {e}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"Ingestion completed: {record_count} records on {PROCESSING_DATE}")