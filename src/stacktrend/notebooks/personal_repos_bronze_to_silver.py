# COMMAND ----------
# MAGIC %%configure -f
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "stacktrend_silver_lh"
# MAGIC     }
# MAGIC }

# COMMAND ----------
# MAGIC %md
# MAGIC # Personal Repository Bronze to Silver Transformation
# MAGIC 
# MAGIC Microsoft Fabric Notebook for processing personal GitHub repository data with LLM classification
# MAGIC 
# MAGIC This notebook transforms raw personal repository data into clean, classified data with metrics.

# COMMAND ----------
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime, timedelta
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("Personal_Repos_Bronze_to_Silver").getOrCreate()

# COMMAND ----------
# Mount additional lakehouses for cross-lakehouse access using secure context
try:
    from notebookutils import mssparkutils
    
    # Get current workspace context securely
    workspace_id = mssparkutils.env.getWorkspaceId()
    
    # Mount Bronze lakehouse using lakehouse name (Fabric resolves the ID securely)
    bronze_mount = "/mnt/bronze"
    bronze_abfs = "abfss://{}@onelake.dfs.fabric.microsoft.com/stacktrend_bronze_lh.Lakehouse".format(workspace_id)
    
    # Check if already mounted
    existing_mounts = [mount.mountPoint for mount in mssparkutils.fs.mounts()]
    if bronze_mount not in existing_mounts:
        mssparkutils.fs.mount(bronze_abfs, bronze_mount)
        print("Mounted Bronze lakehouse at {}".format(bronze_mount))
    else:
        print("Bronze lakehouse already mounted at {}".format(bronze_mount))
        
except Exception as e:
    print("WARNING: Mount failed, will use cross-lakehouse table references: {}".format(e))

# COMMAND ----------
# PARAMETERS CELL: Define parameters that Data Factory will pass
# This cell must be marked as "parameter cell" in Fabric (click ... → Toggle parameter cell)
# Only set if not already defined (prevents overwriting actual values from Data Factory)
if 'azure_openai_api_key' not in locals() or not azure_openai_api_key:  # noqa: F821
    azure_openai_api_key = ""
if 'azure_openai_endpoint' not in locals() or not azure_openai_endpoint:  # noqa: F821
    azure_openai_endpoint = ""
if 'azure_openai_api_version' not in locals() or not azure_openai_api_version:  # noqa: F821
    azure_openai_api_version = "2025-01-01-preview"
if 'azure_openai_model' not in locals() or not azure_openai_model:  # noqa: F821
    azure_openai_model = "o4-mini"

# COMMAND ----------
# SECURE: Configure Azure OpenAI credentials from Data Factory parameters

print("Configuring Azure OpenAI credentials from Data Factory parameters...")

# Validate that required parameters were passed from Data Factory
if not azure_openai_api_key or azure_openai_api_key == "":
    raise ValueError("Azure OpenAI API key is required for LLM classification")

if not azure_openai_endpoint or azure_openai_endpoint == "":
    raise ValueError("Azure OpenAI endpoint is required for LLM classification")

print("✅ Azure OpenAI credentials configured")
print("   Endpoint: {}".format(azure_openai_endpoint))
print("   Model: {}".format(azure_openai_model))
print("   API Version: {}".format(azure_openai_api_version))

# Set environment variables for the LLM classifier (exactly like original)
os.environ['AZURE_OPENAI_API_KEY'] = azure_openai_api_key
os.environ['AZURE_OPENAI_ENDPOINT'] = azure_openai_endpoint
os.environ['AZURE_OPENAI_API_VERSION'] = azure_openai_api_version
os.environ['AZURE_OPENAI_MODEL'] = azure_openai_model

# COMMAND ----------
# Configuration
PROCESSING_DATE = datetime.now().strftime("%Y-%m-%d")
LOOKBACK_DAYS = 30  # For velocity calculations

# COMMAND ----------
# MAGIC %md
# MAGIC ## LLM-Based Repository Classification System

# COMMAND ----------
# ZERO-DEPENDENCY LLM Classification
# Use only what's built into Fabric - no external package installations

def fabric_native_llm_classifier():
    """
    Pure Fabric-native LLM classification using only built-in libraries.
    Avoids ALL dependency conflicts and installation timeouts.
    """
    import urllib.request
    import urllib.parse
    import json
    
    def classify_repo_batch(repo_data_list, api_key, endpoint, model="o4-mini"):
        """Call Azure OpenAI directly using urllib (no external dependencies)."""
        
        # Create the prompt exactly like the original working classifier
        prompt = """You are an expert software engineer analyzing GitHub repositories. Classify these {} repositories into technology categories based on their PRIMARY purpose and functionality.

PRIMARY CATEGORIES (choose exactly one):
- AI: Large Language Models, Generative AI, Agentic AI, MCP Servers, Autonomous Agents, AI Infrastructure
- ML: Machine Learning, Deep Learning, MLOps, Data Science, Computer Vision, NLP, Statistical Models
- DataEngineering: Data Pipelines, ETL/ELT, Streaming, DataOps, Data Mesh, Analytics Engineering, Data Quality
- Databases: SQL/NoSQL databases, Vector Stores, Time Series DBs, Graph Databases, Distributed Systems, Caching
- WebDevelopment: Frontend/Backend, Mobile Apps, APIs, Web Frameworks, Serverless, JAMstack, Progressive Web Apps
- DevOps: CI/CD, Infrastructure as Code, Containerization, Monitoring, GitOps, Cloud Engineering
- CloudServices: Cloud Provider Tools, Serverless Platforms, SaaS SDKs, IaaS/PaaS Tools, Multi-Cloud
- Security: Cybersecurity, DevSecOps, Identity Management, Threat Detection, Cryptography, Zero Trust
- ProgrammingLanguages: Language Implementations, Compilers, Interpreters, Language Servers, Build Systems
- Other: General utilities, gaming, system tools, educational content, miscellaneous

SUBCATEGORY GUIDELINES:
- AI: generative_ai, llm_tools, agentic_ai, mcp_servers, autonomous_agents, ai_infrastructure, reinforcement_learning
- ML: deep_learning, machine_learning, mlops, data_science, model_serving, computer_vision, nlp, statistical_models
- DataEngineering: etl, streaming, orchestration, dataops, data_mesh, analytics_engineering, data_quality, real_time_processing
- Databases: relational, nosql, vector_db, time_series, graph_db, distributed_db, caching, in_memory
- WebDevelopment: frontend, backend, fullstack, mobile, api, web_framework, serverless, jamstack, pwa
- DevOps: containerization, ci_cd, monitoring, infrastructure_as_code, gitops, cloud_engineering, observability
- CloudServices: aws_tools, azure_tools, gcp_tools, multi_cloud, serverless_platforms, saas_tools, paas_tools
- Security: cybersecurity, devsecops, identity_management, threat_detection, cryptography, zero_trust, compliance
- ProgrammingLanguages: language_implementation, compilers, interpreters, language_servers, build_tools, package_managers
- Other: utilities, gaming, system_tools, educational, documentation, testing_tools

ENHANCED CLASSIFICATION RULES:
1. Analyze repository NAME, DESCRIPTION, TOPICS, and PRIMARY LANGUAGE to understand core purpose
2. Prioritize the MAIN functionality over supporting technologies (e.g., a web app using ML is WebDevelopment, not ML)
3. Consider modern patterns: AI-first applications, cloud-native architectures, serverless designs
4. Key indicators to look for:
   - AI/ML: model training, inference, transformers, neural networks, AI agents
   - DataEngineering: pipeline, ETL, streaming, kafka, airflow, spark, data lake
   - Security: auth, encryption, security scanning, vulnerability, penetration testing
   - CloudServices: AWS/Azure/GCP in name, terraform, kubernetes operators for cloud
5. Confidence scoring:
   - 0.9+: Clear technology stack and purpose evident
   - 0.8+: Strong indicators with minor ambiguity
   - 0.7+: Reasonable certainty based on available information
   - 0.6+: Educated guess with limited information
6. Avoid "Other" unless truly unclear or doesn't fit established patterns

MODERN TECHNOLOGY INDICATORS:
- Vector databases, embeddings → Databases (vector_db)
- LLM applications, RAG systems → AI (llm_tools)
- Data mesh, DataOps → DataEngineering (data_mesh, dataops)
- GitOps, ArgoCD → DevOps (gitops)
- JAMstack, Edge computing → WebDevelopment (jamstack)
- Zero Trust, SIEM → Security (zero_trust, threat_detection)
- MCP protocol implementations, Model Context Protocol → AI (mcp_servers)

Repositories to classify:
""".format(len(repo_data_list))
        
        for i, repo in enumerate(repo_data_list, 1):
            topics_str = ", ".join(repo.get('topics', [])[:5]) if repo.get('topics') else "none"
            description = (repo.get('description', '') or "no description")[:200]
            
            prompt += """
{}. ID: {}
   Name: {}
   Description: {}
   Topics: [{}]
   Language: {}
   Stars: {}
""".format(
                i, repo['repository_id'], repo['name'], 
                description, topics_str, 
                repo.get('language', 'unknown'), repo.get('stargazers_count', 0)
            )
        
        prompt += """

Return ONLY a valid JSON object with a "classifications" array:
{{
    "classifications": [
        {{
            "repo_id": "123",
            "primary_category": "AI",
            "subcategory": "llm_tools", 
            "confidence": 0.95
        }}
    ]
}}

Must return exactly {} classification objects here.""".format(len(repo_data_list))
        
        # Make HTTP request to Azure OpenAI
        url = "{}/openai/deployments/{}/chat/completions?api-version=2025-01-01-preview".format(
            endpoint.rstrip('/'), model
        )
        
        headers = {
            "api-key": api_key,
            "Content-Type": "application/json"
        }
        
        # API call info
        print("Calling Azure OpenAI for {} repositories".format(len(repo_data_list)))
        
        payload = {
            "messages": [
                {"role": "system", "content": "You are a precise repository classifier. Return only valid JSON arrays as requested."},
                {"role": "user", "content": prompt}
            ],
            "max_completion_tokens": 4000,  # Increased for longer responses
            "response_format": {"type": "json_object"}
        }
        
        # Use urllib.request for HTTP call (built into Python)
        req = urllib.request.Request(
            url, 
            data=json.dumps(payload).encode('utf-8'),
            headers=headers,
            method='POST'
        )
        
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                # Read response body ONCE
                response_body = response.read().decode()
                
                if response.status != 200:
                    raise Exception("Azure OpenAI API error {}: {}".format(response.status, response_body))
                
                # Parse the response
                result = json.loads(response_body)
                content = result['choices'][0]['message']['content']
                
                # Check for truncation
                finish_reason = result['choices'][0].get('finish_reason', '')
                if finish_reason == 'length':
                    raise Exception("Response truncated due to token limit. Reduce batch size or increase max_completion_tokens.")
                
                if not content or content.strip() == "":
                    raise Exception("LLM returned empty content. Finish reason: {}".format(finish_reason))
                
                return json.loads(content)
                
        except urllib.error.HTTPError as e:
            # Read the error response body for debugging
            error_body = e.read().decode() if hasattr(e, 'read') else str(e)
            print("Azure OpenAI HTTP Error {}: {}".format(e.code, error_body))
            raise Exception("Azure OpenAI API error {}: {}".format(e.code, error_body))
        except Exception as e:
            raise Exception("Azure OpenAI API call failed: {}".format(str(e)))
    
    return classify_repo_batch

def classify_repositories_with_llm(repositories_df):
    """
    Smart LLM classification - only classify repos that need it (not already well-classified)
    """
    # Get the zero-dependency classifier
    classify_repo_batch = fabric_native_llm_classifier()
    
    try:
        # Check existing Silver data to avoid re-classifying well-classified repos
        repos_needing_llm = repositories_df
        repos_for_metrics_only = spark.createDataFrame([], repositories_df.schema)
        
        try:
            existing_silver_df = spark.table("github_my_portfolio")
            
            # Get repos that are already well-classified (confidence >= 0.8 and not Other/unknown)
            well_classified = (existing_silver_df
                .filter(
                    (F.col("technology_category") != "Other") & 
                    (F.col("technology_subcategory") != "unknown") &
                    (F.col("classification_confidence") >= 0.8)
                )
                .select("repository_id", "technology_category", "technology_subcategory", "classification_confidence")
            )
            
            # Split repos: those needing LLM vs those needing metrics-only update  
            repos_needing_llm = repositories_df.join(well_classified, "repository_id", "left_anti")
            repos_for_metrics_only = repositories_df.join(well_classified, "repository_id", "inner")
            
            llm_count = repos_needing_llm.count()
            metrics_count = repos_for_metrics_only.count()
            
            print("Smart Classification Strategy:")
            print("  Repos needing LLM classification: {}".format(llm_count))
            print("  Repos with metrics-only update: {}".format(metrics_count))
            print("  Cost savings: ~${:.3f} (skipped {} LLM calls)".format(metrics_count * 0.00006, metrics_count))
            
        except Exception as e:
            print("No existing Silver data found: {}".format(e))
            print("Will classify all repositories with LLM")
        
        # Only run LLM on repos that need it
        if repos_needing_llm.count() == 0:
            print("No repositories need LLM classification")
            return repositories_df.withColumn("technology_category", F.lit("Other")).withColumn("technology_subcategory", F.lit("unknown")).withColumn("classification_confidence", F.lit(0.1))
        
        # Convert Spark DataFrame to list of repository data (only for repos needing LLM)
        repo_data_list = []
        repos_collected = repos_needing_llm.collect()
        
        for row in repos_collected:
            # Convert PySpark Row to dict for safe access
            row_dict = row.asDict()
            repo_data_list.append(row_dict)  # Just use the dict directly
        
        print("Running LLM classification on {} repositories...".format(repos_needing_llm.count()))
        
        # Use zero-dependency LLM classification with smaller batches to avoid token limits
        try:
            # Process in small batches to avoid token limit issues
            batch_size = 3  # Small batches to ensure responses fit in token limit
            all_classifications = []
            
            for i in range(0, len(repo_data_list), batch_size):
                batch = repo_data_list[i:i + batch_size]
                print("Processing batch {}: {} repositories".format(i//batch_size + 1, len(batch)))
                
                batch_response = classify_repo_batch(
                    batch, 
                    azure_openai_api_key, 
                    azure_openai_endpoint, 
                    azure_openai_model
                )
                
                # Parse classifications from batch response
                if isinstance(batch_response, dict) and 'classifications' in batch_response:
                    batch_classifications = batch_response['classifications']
                elif isinstance(batch_response, list):
                    batch_classifications = batch_response
                else:
                    raise Exception("Unexpected response format from Azure OpenAI")
                
                all_classifications.extend(batch_classifications)
            
            classifications = all_classifications
                
        except Exception as e:
            print("LLM classification failed: {}".format(str(e)))
            raise Exception("Azure OpenAI API error: {}".format(str(e)))
        
        # Convert results to classification map
        classification_map = {}
        for classification in classifications:
            repo_id = str(classification.get('repo_id', ''))
            classification_map[repo_id] = {
                'primary_category': classification.get('primary_category', 'Other'),
                'subcategory': classification.get('subcategory', 'unknown'),
                'confidence': float(classification.get('confidence', 0.1))
            }
        
        # Add classifications to original DataFrame
        def add_classification(repo_id):
            str_repo_id = str(repo_id)
            classification = classification_map.get(str_repo_id, {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return classification['primary_category']

        def add_subcategory(repo_id):
            str_repo_id = str(repo_id)
            classification = classification_map.get(str_repo_id, {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return classification['subcategory']

        def add_confidence(repo_id):
            str_repo_id = str(repo_id)
            classification = classification_map.get(str_repo_id, {
                'primary_category': 'Other',
                'subcategory': 'unknown',
                'confidence': 0.1
            })
            return float(classification['confidence'])
        
        add_classification_udf = F.udf(add_classification, StringType())
        add_subcategory_udf = F.udf(add_subcategory, StringType())
        add_confidence_udf = F.udf(add_confidence, DoubleType())
        
        # Add classifications to repos that needed LLM
        classified_repos = (repos_needing_llm
                .withColumn("technology_category", add_classification_udf(F.col("repository_id")))
                .withColumn("technology_subcategory", add_subcategory_udf(F.col("repository_id")))
                .withColumn("classification_confidence", add_confidence_udf(F.col("repository_id"))))
        
        # For repos with metrics-only update, preserve existing classifications
        if repos_for_metrics_only.count() > 0:
            # Create lookup map for existing classifications
            existing_classifications = {row.repository_id: (row.technology_category, row.technology_subcategory, row.classification_confidence) 
                                     for row in well_classified.collect()}
            
            def get_existing_category(repo_id):
                return existing_classifications.get(repo_id, ("Other", "unknown", 0.1))[0]
            
            def get_existing_subcategory(repo_id):
                return existing_classifications.get(repo_id, ("Other", "unknown", 0.1))[1]
                
            def get_existing_confidence(repo_id):
                return existing_classifications.get(repo_id, ("Other", "unknown", 0.1))[2]
            
            # Create UDFs for existing classifications  
            existing_category_udf = F.udf(get_existing_category, StringType())
            existing_subcategory_udf = F.udf(get_existing_subcategory, StringType()) 
            existing_confidence_udf = F.udf(get_existing_confidence, DoubleType())
            
            # Add existing classifications using same pattern as LLM classifications
            metrics_with_classification = (repos_for_metrics_only
                .withColumn("technology_category", existing_category_udf(F.col("repository_id")))
                .withColumn("technology_subcategory", existing_subcategory_udf(F.col("repository_id")))
                .withColumn("classification_confidence", existing_confidence_udf(F.col("repository_id"))))
            
            # Union both DataFrames
            final_classified_df = classified_repos.union(metrics_with_classification)
        else:
            final_classified_df = classified_repos
        
        print("✅ Repository classification completed")
        return final_classified_df
        
    except Exception as e:
        print("Repository classification failed: {}".format(e))
        raise Exception("LLM classification failed: {}".format(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Loading from Bronze Layer

# COMMAND ----------
# Read Personal repositories data from Bronze layer
try:
    # Try mounted path first, fallback to cross-lakehouse reference
    # Use cross-lakehouse table reference directly (same as original working notebooks)
    repos_bronze_df = spark.table("stacktrend_bronze_lh.github_my_repos")
    print("SUCCESS: Successfully loaded personal repos using cross-lakehouse reference")
    
    # Show basic statistics
    total_repos = repos_bronze_df.count()
    print("Bronze Personal Repos Data: {} repositories".format(total_repos))
    
except Exception as e:
    print("Error loading repository data: {}".format(e))
    raise Exception("Cannot proceed without Bronze repository data: {}".format(e))

# Read Activity data from Bronze layer
try:
    # Use cross-lakehouse table reference directly (same as original working notebooks)
    activity_bronze_df = spark.table("stacktrend_bronze_lh.github_repo_activity")
    print("SUCCESS: Successfully loaded activity using cross-lakehouse reference")
    
    total_activities = activity_bronze_df.count()
    print("Bronze Activity Data: {} activities".format(total_activities))
    
except Exception as e:
    print("Error loading activity data: {}".format(e))
    raise Exception("Cannot proceed without Bronze activity data: {}".format(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Repository Data Cleaning and Enrichment

# COMMAND ----------
if repos_bronze_df is not None:
    print("Processing personal repository data...")
    
    # Clean and standardize repository data
    repos_clean_df = repos_bronze_df.select(
        F.col("repository_id"),
        F.col("name"),
        F.col("full_name"),
        F.col("description"),
        F.col("private"),
        F.col("language").alias("primary_language"),
        F.col("stargazers_count"),
        F.col("forks_count"),
        F.col("open_issues_count"),
        F.col("size"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("pushed_at"),
        F.col("topics"),
        F.col("license_name"),
        F.col("default_branch"),
        F.col("partition_date")
    ).filter(
        F.col("repository_id").isNotNull()
    )
    
    # Clean description
    repos_clean_df = repos_clean_df.withColumn(
        "description_clean",
        F.when(F.col("description").isNotNull(), 
               F.trim(F.regexp_replace(F.col("description"), "[\\r\\n\\t]+", " ")))
         .otherwise("No description provided")
    )
    
    # Calculate derived metrics
    repos_clean_df = repos_clean_df.withColumn(
        "repository_size_mb", (F.col("size") / 1024.0).cast(DoubleType())
    ).withColumn(
        "days_since_creation", 
        F.datediff(F.current_date(), F.col("created_at").cast("date"))
    ).withColumn(
        "days_since_last_push",
        F.datediff(F.current_date(), F.col("pushed_at").cast("date"))
    ).withColumn(
        "is_active",
        F.when(F.col("days_since_last_push") <= 30, True).otherwise(False)
    ).withColumn(
        "visibility",
        F.when(F.col("private"), "private").otherwise("public")
    )
    
    # Calculate quality score based on multiple factors
    repos_clean_df = repos_clean_df.withColumn(
        "quality_score",
        F.least(F.lit(1.0),
            (F.when(F.col("description").isNotNull() & (F.length(F.col("description")) > 10), 0.2).otherwise(0.0) +
             F.when(F.col("license_name").isNotNull(), 0.2).otherwise(0.0) +
             F.when(F.size(F.col("topics")) > 0, 0.2).otherwise(0.0) +
             F.when(F.col("stargazers_count") > 0, 0.2).otherwise(0.0) +
             F.when(F.col("is_active"), 0.2).otherwise(0.0))
        ).cast(DoubleType())
    )
    
    # Determine popularity tier
    repos_clean_df = repos_clean_df.withColumn(
        "popularity_tier",
        F.when(F.col("stargazers_count") >= 100, "high")
         .when(F.col("stargazers_count") >= 10, "medium")
         .otherwise("low")
    )
    
    # Extract language distribution (simplified)
    repos_clean_df = repos_clean_df.withColumn(
        "language_distribution",
        F.when(F.col("primary_language").isNotNull(), 
               F.create_map(F.col("primary_language"), F.lit(1.0)))
         .otherwise(F.create_map())
    )
    
    # Apply LLM classification
    print("Applying LLM-based technology classification...")
    repos_classified_df = classify_repositories_with_llm(repos_clean_df)
    
    # Add processing metadata
    repos_final_df = repos_classified_df.withColumn(
        "processed_timestamp", F.lit(datetime.now())
    )
    
    print("Personal repositories processed: {} records".format(repos_final_df.count()))
    
else:
    print("No repository data available for processing")
    repos_final_df = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Activity Metrics Calculation

# COMMAND ----------
if activity_bronze_df is not None and repos_final_df is not None:
    print("Calculating activity metrics...")
    
    # Calculate activity metrics for different periods
    activity_periods = ["7d", "30d", "90d"]
    all_activity_metrics = []
    
    for period in activity_periods:
        days = int(period[:-1])
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Filter activity for this period
        period_activity = activity_bronze_df.filter(
            F.col("activity_date") >= F.lit(cutoff_date)
        )
        
        # Calculate metrics per repository
        repo_activity_metrics = period_activity.groupBy("repository_id").agg(
            F.sum(F.when(F.col("activity_type") == "commit", 1).otherwise(0)).alias("total_commits"),
            F.sum(F.when(F.col("activity_type") == "issue", 1).otherwise(0)).alias("total_issues"),
            F.sum(F.when(F.col("activity_type") == "release", 1).otherwise(0)).alias("total_releases"),
            F.sum(F.coalesce(F.col("additions"), F.lit(0))).alias("lines_added"),
            F.sum(F.coalesce(F.col("deletions"), F.lit(0))).alias("lines_deleted"),
            F.sum(F.coalesce(F.col("changed_files"), F.lit(0))).alias("files_changed"),
            F.max("activity_date").alias("last_activity_date")
        ).withColumn(
            "measurement_period", F.lit(period)
        ).withColumn(
            "commit_frequency", (F.col("total_commits") / F.lit(days)).cast(DoubleType())
        ).withColumn(
            "issue_resolution_rate", 
            F.when(F.col("total_issues") > 0, 
                   F.col("total_issues") / F.greatest(F.lit(1), F.col("total_issues"))).otherwise(0.0)
        ).withColumn(
            "development_velocity",
            F.least(F.lit(1.0),
                (F.col("commit_frequency") * 0.4 +
                 F.least(F.lit(1.0), F.col("lines_added") / 1000.0) * 0.3 +
                 F.least(F.lit(1.0), F.col("files_changed") / 100.0) * 0.3)).cast(DoubleType())
        ).withColumn(
            "activity_trend",
            F.when(F.col("development_velocity") >= 0.7, "increasing")
             .when(F.col("development_velocity") >= 0.3, "stable")
             .otherwise("decreasing")
        ).withColumn(
            "processed_timestamp", F.lit(datetime.now())
        ).withColumn(
            "partition_date", F.lit(PROCESSING_DATE)
        )
        
        all_activity_metrics.append(repo_activity_metrics)
    
    # Union all periods
    if all_activity_metrics:
        activity_metrics_df = all_activity_metrics[0]
        for df in all_activity_metrics[1:]:
            activity_metrics_df = activity_metrics_df.union(df)
        
        print("Activity metrics calculated: {} records".format(activity_metrics_df.count()))
    else:
        activity_metrics_df = None
        
else:
    print("No activity data available for metrics calculation")
    activity_metrics_df = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Personal Portfolio to Silver Layer

# COMMAND ----------
if repos_final_df is not None:
    print("Saving personal portfolio to Silver layer...")
    
    # Select final columns for Silver layer
    portfolio_silver_df = repos_final_df.select(
        "repository_id",
        "name",
        "full_name",
        "description_clean",
        "technology_category",
        "technology_subcategory",
        "classification_confidence",
        "primary_language",
        "language_distribution",
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "repository_size_mb",
        "days_since_creation",
        "days_since_last_push",
        "is_active",
        "visibility",
        "quality_score",
        "popularity_tier",
        "processed_timestamp",
        "partition_date"
    )
    
    # Smart Delta merge to Silver lakehouse using SQL MERGE pattern
    try:
        # Check if Silver table exists
        table_exists = True
        try:
            existing_silver = spark.table("github_my_portfolio")
            existing_silver_count = existing_silver.count()
            print("Found existing Silver portfolio table with {} records".format(existing_silver_count))
        except Exception as e:
            table_exists = False
            print("Silver portfolio table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        clean_records = portfolio_silver_df.count()
        
        if table_exists and clean_records > 0:
            print("Performing Personal Portfolio Silver layer Delta merge...")
            
            portfolio_silver_df.createOrReplaceTempView("new_portfolio_silver_data")
            
            # SMART MERGE: Preserve good classifications, update metrics
            merge_sql = """
            MERGE INTO github_my_portfolio AS target
            USING new_portfolio_silver_data AS source
            ON target.repository_id = source.repository_id
            
            WHEN MATCHED THEN
              UPDATE SET
                name = source.name,
                full_name = source.full_name,
                description_clean = source.description_clean,
                primary_language = source.primary_language,
                language_distribution = source.language_distribution,
                stargazers_count = source.stargazers_count,
                forks_count = source.forks_count,
                open_issues_count = source.open_issues_count,
                repository_size_mb = source.repository_size_mb,
                days_since_creation = source.days_since_creation,
                days_since_last_push = source.days_since_last_push,
                is_active = source.is_active,
                visibility = source.visibility,
                quality_score = source.quality_score,
                popularity_tier = source.popularity_tier,
                processed_timestamp = source.processed_timestamp,
                partition_date = source.partition_date,
                
                -- Only update technology fields if current classification is poor
                technology_category = CASE 
                    WHEN target.technology_category = 'Other' OR target.technology_category IS NULL 
                    THEN source.technology_category 
                    ELSE target.technology_category 
                END,
                technology_subcategory = CASE 
                    WHEN target.technology_subcategory = 'unknown' OR target.technology_subcategory IS NULL 
                    THEN source.technology_subcategory 
                    ELSE target.technology_subcategory 
                END,
                classification_confidence = CASE 
                    WHEN target.technology_category = 'Other' OR target.technology_subcategory = 'unknown' 
                    THEN source.classification_confidence 
                    ELSE target.classification_confidence 
                END
                
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            
            # Get merge statistics
            final_silver_count = spark.table("github_my_portfolio").count()
            new_records = final_silver_count - existing_silver_count
            updated_records = clean_records - max(0, new_records)
            
            print("Personal Portfolio Silver layer Delta merge completed:")
            print("  Total records now: {}".format(final_silver_count))
            print("  New records added: {}".format(max(0, new_records)))
            print("  Records updated: {}".format(updated_records))
            
        else:
            # First time or no data - use overwrite
            if clean_records > 0:
                (portfolio_silver_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date", "technology_category")
                 .saveAsTable("github_my_portfolio"))
                print("Created new Silver portfolio table with {} records".format(clean_records))
            else:
                print("No clean portfolio records to save")
        
    except Exception as e:
        print("Error with Portfolio Silver Delta merge: {}".format(e))
        print("Falling back to overwrite mode...")
        try:
            (portfolio_silver_df
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .partitionBy("partition_date", "technology_category")
             .saveAsTable("github_my_portfolio"))
            print("Fallback successful: Saved {} records".format(clean_records))
        except Exception as fallback_error:
            print("Fallback also failed: {}".format(fallback_error))
            raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Activity Metrics to Silver Layer

# COMMAND ----------
if activity_metrics_df is not None:
    print("Saving activity metrics to Silver layer...")
    
    # Smart Delta merge to Silver lakehouse using SQL MERGE pattern
    try:
        # Check if table exists
        table_exists = True
        try:
            existing_activity = spark.table("github_activity_metrics")
            existing_count = existing_activity.count()
            print("Found existing activity metrics table with {} records".format(existing_count))
        except Exception as e:
            table_exists = False
            print("Activity metrics table doesn't exist - will create new table")
            print("Table check error: {}".format(e))
        
        metrics_records = activity_metrics_df.count()
        
        if table_exists and metrics_records > 0:
            print("Performing Activity Metrics Delta merge...")
            
            activity_metrics_df.createOrReplaceTempView("new_activity_metrics_data")
            
            # SQL MERGE: Update activity metrics
            merge_sql = """
            MERGE INTO github_activity_metrics AS target
            USING new_activity_metrics_data AS source
            ON target.repository_id = source.repository_id AND target.measurement_period = source.measurement_period
            
            WHEN MATCHED THEN
              UPDATE SET *
              
            WHEN NOT MATCHED THEN
              INSERT *
            """
            
            spark.sql(merge_sql)
            print("✅ Activity metrics merged successfully")
            
        else:
            # First time - create new table
            if metrics_records > 0:
                (activity_metrics_df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy("partition_date", "measurement_period")
                 .saveAsTable("github_activity_metrics"))
                print("Created new activity metrics table with {} records".format(metrics_records))
            else:
                print("No activity metrics to save")
        
    except Exception as e:
        print("Error with Activity Metrics Delta merge: {}".format(e))
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("Personal Repository Bronze to Silver Transformation Complete!")
print("Processing Date: {}".format(PROCESSING_DATE))
print("\nSummary:")

if repos_final_df is not None:
    total_repos = repos_final_df.count()
    active_repos = repos_final_df.filter(F.col("is_active")).count()
    print("   Repositories processed: {}".format(total_repos))
    print("   Active repositories: {}".format(active_repos))

if activity_metrics_df is not None:
    total_metrics = activity_metrics_df.count()
    print("   Activity metrics calculated: {}".format(total_metrics))


print("\nData ready for Gold layer analytics!")

# COMMAND ----------
# Clean up
spark.stop()
