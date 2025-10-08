# YouTube Trending Data Pipeline

## Overview
This project implements a fully automated **YouTube trending video data pipeline** using **`Python`**, **`Apache Airflow`**, and **`Google Cloud Platform`**. It fetches daily trending videos via the **YouTube Data API**, stores raw **JSON** files in **`Google Cloud Storage`** (acting as the **`Data Lake`**) for immutable storage, and loads structured tables into **`BigQuery`** (serving as the **`Data Warehouse`**) for efficient querying and analytics.

The pipeline also performs automated transformations to generate key metrics such as top category per region, total views/likes, and engagement ratios per 1000 views, and sends **automated** weekly summary reports via **email**.

This project demonstrates **End-to-End Data Engineering** best practices, including scalable ingestion, **`ETL`** processes, cloud storage architecture, analytics, and automated reporting.

---

## Tech Stack
- **Languages:** Python, SQL  
- **Workflow Orchestration:** Apache Airflow (Google Cloud Composer)  
- **Cloud Services:** Google Cloud Storage (GCS), BigQuery, Gmail API  
- **APIs:** YouTube Data API v3  
- **Libraries:** Pandas, Requests, Google Cloud SDK, BigQuery Client Library  
- **Reporting:** HTML Email Automation with sendgrid.

---

## Skills Demonstrated
- **Python Programming**: API integration, data processing, and modular utility functions.
- **Apache Airflow**: DAG creation, scheduling, parameterized manual triggers, and task orchestration.
- **Google Cloud Platform**:
   - **GCS**: Raw JSON storage.
   - **BigQuery**: Raw and analytics datasets for structured data and insights.
   - **GCP Composer**: Managed AirFlow environment to orchestrate the workflow (ingestion, transformation, aggregation, and reporting).

- **Data Engineering Best Practices**:
   - **ETL Pipeline Design**: End-to-end automation from data ingestion to reporting.
   - **Separation of Layers**: Clear distinction between **Data Lake** (raw storage) and **Data Warehouse** (analytics).
   - **Parameterization & Scalability**: Supports multi-region ingestion through flexible, parameterized DAGs.
   - **Data Quality & Consistency**: Controlled ingestion and transformation ensuring reliability across all layers.

- **Reporting & Automation**: Automated weekly email summaries with top insights and engagement metrics.

---

## Project Architecture

This project is built around three core layers **(Ingestion)**, **(Storage & Processing)**, and **(Analytics & Reporting)** all orchestrated by **`Apache Airflow`** running on **`Google Cloud Composer`**.

Components:

- **YouTube Data API**: External data source providing trending videos.
- **Google Cloud Composer (Airflow)**: Manages all pipeline workflows.
- **Google Cloud Storage (GCS)**: Stores raw JSON files.
- **BigQuery (Raw Dataset)**: Stores structured daily trending videos and channels data.
- **BigQuery (Analytics Dataset)**: Stores aggregated insights (daily per region).
- **Email Service**: Sends automated weekly summaries and insights.

---

## Project Flow
This pipeline automates YouTube trending data collection, transformation, and reporting, fully orchestrated with **`Apache Airflow`** on **`Google Cloud Composer`**.

**1️⃣ Data Ingestion (Daily)**

- The first DAG fetches trending videos from the **YouTube Data API** for **multiple regions**.
- **Raw JSON** files are stored in **Google Cloud Storage (GCS)**.
- The DAG then parses and loads the structured data into **BigQuery** (Raw Dataset) tables:
  - **daily_trending_videos**: contains the trending videos' data. 
  - **channels**: contains the videos' channels data like (subscribres count, channel's categort, etc) for further analysis.

**2️⃣ Data Aggregation (Daily)**
- The second DAG reads from the Raw Dataset, computes daily metrics per region (total and average views, likes, comments, and engagement ratio), and stores the results in the Analytics Dataset (daily_insights table) in **BigQuery**.

**3️⃣ Weekly Reporting (Scheduled + Manual)**
- The final DAG runs weekly (or manually on-demand) to summarize the latest insights.
- It identifies top categories, total engagement, and growth trends per region, then emails a formatted HTML summary report automatically.

--- 

## Use Case
This pipeline provides daily and weekly insights into YouTube’s trending ecosystem.  
It enables content analysts, marketers, and media strategists to:
- Identify top-performing categories by region.
- Track engagement trends over time.
- Measure audience preferences across countries.
- Automate analytics reporting with zero manual effort.

---
## Sample
