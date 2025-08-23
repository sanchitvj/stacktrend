# StackTrend Technology Adoption Observatory

A comprehensive data engineering solution for analyzing GitHub repository trends and technology adoption patterns using Microsoft Fabric, Azure OpenAI, and advanced analytics pipelines.

## Overview

StackTrend is an intelligent data platform that combines automated data collection, LLM-powered classification, and real-time analytics to provide insights into open source technology ecosystems. The platform monitors GitHub repositories, classifies them using Azure OpenAI, and generates actionable business intelligence through automated data pipelines.

## Architecture

![Architecture Overview](public/fabric.gif)

The platform implements a modern medallion architecture pattern within Microsoft Fabric, creating a seamless flow from raw data collection to business intelligence dashboards.

[Youtube video](https://youtu.be/_owR-jd8nlY) of project demo I gave at an Microsoft event. 

### Data Flow

1. **Data Collection Layer**: GitHub API serves as the primary data source, providing repository metadata, statistics, and activity information
2. **Orchestration Layer**: Microsoft Fabric Data Factory manages pipeline scheduling, parameter passing, and workflow dependencies
3. **Processing Layer**: Fabric Notebooks execute PySpark transformations, integrate with Azure OpenAI for intelligent classification, and implement business logic
4. **Storage Layer**: Lakehouse architecture with Bronze, Silver, and Gold layers using Delta Lake for ACID compliance and schema evolution
5. **Semantic Layer**: Direct Lake connections provide optimized data models for analytical workloads
6. **Presentation Layer**: Power BI dashboards deliver real-time insights with automated refresh capabilities

### Technical Architecture

**Bronze Layer (Raw Data)**
- GitHub API responses stored in their original format
- Minimal transformation, preserving data lineage
- Delta Lake format for efficient querying and time travel

**Silver Layer (Curated Data)**
- Data cleaning, standardization, and quality validation
- Azure OpenAI integration for intelligent repository classification
- Smart classification logic that avoids redundant LLM calls, reducing costs by 90%
- Technology categorization across AI, ML, Data Engineering, Web Development, DevOps, and other domains

**Gold Layer (Analytics-Ready)**
- Pre-aggregated metrics and KPIs optimized for dashboard performance
- Technology trend analysis, adoption lifecycle classification, and market intelligence
- Repository health scoring and comparative analytics
- Time-series data for historical trend analysis

## Key Features

**Intelligent Classification System**
- LLM-powered repository categorization using Azure OpenAI GPT-4o Mini
- Advanced prompt engineering for accurate technology classification
- Confidence scoring and drift detection for classification quality
- Cost optimization through conditional classification logic

**Real-Time Analytics Pipeline**
- Automated data collection and processing every 2 hours
- Delta Lake merge operations for efficient data updates
- Cross-lakehouse access patterns for complex analytical queries
- Automated Power BI dataset refresh with pipeline completion triggers

**Scalable Data Architecture**
- Microsoft Fabric unified platform eliminating traditional data silos
- Lakehouse storage providing both analytical and transactional capabilities
- PySpark processing for big data workloads
- CI/CD integration with GitHub Actions for automated notebook deployment

**Business Intelligence Integration**
- Pre-built Power BI dashboards for technology trends and repository analytics
- Real-time KPI monitoring with configurable refresh intervals
- Interactive filtering and drill-down capabilities
- Mobile-optimized visualizations for executive reporting

## Implementation Highlights

**Cost Optimization**
- Smart classification reduces Azure OpenAI API calls by approximately 90%
- Delta Lake merge operations minimize storage costs through efficient data updates
- Automated resource scaling based on workload requirements

**Data Quality Assurance**
- Comprehensive validation rules at each processing layer
- Automated data quality monitoring with anomaly detection
- Schema evolution support for changing data structures
- Audit trails for data lineage and compliance

**Operational Excellence**
- Zero-dependency implementations for challenging Fabric environments
- Robust error handling with detailed logging and monitoring
- Automated deployment pipelines with environment promotion
- Performance optimization through strategic data partitioning

## Technology Stack

- **Microsoft Fabric**: Unified analytics platform providing lakehouse, notebooks, and orchestration
- **Azure OpenAI**: GPT-4o Mini for intelligent repository classification
- **Delta Lake**: ACID-compliant storage layer with time travel capabilities
- **Apache Spark**: Distributed processing engine for large-scale data transformations
- **Power BI**: Business intelligence platform with real-time dashboard capabilities
- **Python/PySpark**: Primary development languages with extensive library ecosystem
- **GitHub Actions**: CI/CD pipeline for automated deployment and testing
