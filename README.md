# YouTube Trending Data Pipeline

## Overview
This project implements a fully automated **YouTube trending video data pipeline** using **`Python`**, **`Apache Airflow`**, and **`Google Cloud Platform`**. It fetches daily trending videos via the **YouTube Data API**, stores raw **JSON** files in **`Google Cloud Storage`** (acting as the **`Data Lake`**) for immutable storage, and loads structured tables into **`BigQuery`** (serving as the **`Data Warehouse`**) for efficient querying and analytics.

The pipeline also performs automated transformations to generate key metrics such as top category per region, total views/likes, and engagement ratios per 1000 views, and sends **automated** weekly summary reports via **email**.

This project demonstrates **End-to-End Data Engineering** best practices, including scalable ingestion, **`ETL`** processes, cloud storage architecture, analytics, and automated reporting.

---

## Skills Demonstrated
- **Python Programming**: API integration, data processing, and modular utility functions.
- **Airflow (Cloud Composer)**: DAG creation, scheduling, parameterized manual triggers, and task orchestration.
- **Google Cloud Platform**:
 - **GCS**: Raw JSON storage.
 - **BigQuery**: Raw and analytics datasets for structured data and insights.

- ETL pipeline design

Separation of raw vs analytics layers

Parameterization and scalability

Reporting & Automation: Generating automated weekly summaries via email.
