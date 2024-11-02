# Spark Transform & Analysis Pipeline with Airflow

This repository provides a data pipeline for loading, transforming, and analyzing e-commerce data using Apache Spark and Apache Airflow. The pipeline reads data from a CSV file, loads it into PostgreSQL, performs transformations, and extracts insights directly into Spark DataFrames for analysis. **All of the outputs of this code (include screenshots) were saved in output folder**.

# Project Overview

The goal of this project is to process, transform, and analyze e-commerce data using Apache Spark for transformations and aggregations, with Apache Airflow managing task orchestration. The data is read from local then loads the data into a PostgreSQL Database, after that performs data transformations and aggregations using Spark Dataframes on the postgreSQL data. Last, the data were extracted to find insights based on specific business questions directly from Spark DataFrames.

# Analysis Goals

This project aims to address the following key questions through data transformation and aggregation:

1. Top Sellers by Order Count
2. Top Product Categories by Sales Volume
3. Top Cities by Customer Orders

# Data Flow

1. Data Load: Load raw data into PostgreSQL using Apache Spark.
2. Data Transformation: Use Spark DataFrames to perform joins, aggregations, and filtering based on the goals outlined above.
3. Insight Extraction: Extract insight from the processed spark dataframes for analysis.

# Usage

1. Clone This Repo.
2. Run `make docker-build`

## list code

```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## postgres                     - Run a Postgres container
## spark                        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## jupyter                      - Spinup jupyter notebook for testing and validation purposes.
## airflow                      - Spinup airflow scheduler and webserver.
## postgres-sql                 - Run psql in Postgres container
```

---

Reference from seceng with some improvement
