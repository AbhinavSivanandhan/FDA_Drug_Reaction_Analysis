Here’s a polished `README.md` tailored for showcasing your **FDA Drug Reaction Analysis** project on GitHub — perfect for a resume or portfolio link:

---

# 💊 FDA Drug Reaction Analysis  
**Distributed ETL & Exploratory Analysis on 500GB+ of Adverse Drug Event Data**  
*Dask · Spark · DuckDB · PyArrow · Parquet*

---

## 📌 Project Overview

This project tackles large-scale healthcare data engineering and analysis using distributed tools. It processes over **500GB of nested JSON** from the [FDA OpenFDA](https://open.fda.gov/) database, focusing on adverse drug event (ADE) reports.

We built a **distributed ETL pipeline** using **Dask**, **PyArrow**, and **Apache Spark** to flatten, clean, and convert nested JSON into **Parquet**, enabling scalable querying and exploratory analysis with tools like **DuckDB** and **Pandas**.

---

## ⚙️ Tech Stack

| Tool       | Purpose                                         |
|------------|-------------------------------------------------|
| **Dask**   | File handling, parallel flattening & filtering  |
| **Spark**  | Distributed ETL and Parquet conversion          |
| **PyArrow**| High-performance JSON → Parquet conversion      |
| **DuckDB** | Fast local SQL on Parquet files                 |
| **Pandas** | Local analysis and visualization                |
| **Snappy** | Compression for storage-efficient Parquet files |

---

## 📂 Dataset

- **Source**: FDA's OpenFDA Drug Event JSON dataset  
- **Raw Size**: ~102 GB (compressed)  
- **Expanded Size**: ~516 GB (uncompressed)  
- **Structure**: 1500+ deeply nested `.json` files  
- **Schema Includes**:  
  - `drugname`, `reactionmeddrapt`, `safetyreportid`, `seriousness`, `patient`, `drug`, `reactions`, `reportercountry`, etc.

---

## 🔧 Pipeline Steps

1. **Unzipping & JSON Extraction**:  
   Extracted 1500+ `.json` files from `.zip` archives using parallelized Dask routines.

2. **Data Cleaning**:  
   - Removed redundant fields (e.g. `rxcui`, `spl_id`, `package_ndc`)  
   - Skipped and logged corrupted/empty files  
   - Flattened nested structures for downstream processing

3. **Conversion to Parquet**:  
   - Used PyArrow + Dask to convert flattened JSON into Snappy-compressed Parquet  
   - Reduced size from 500+ GB to ~15 GB  
   - Stored results in partitioned directories for efficient query access

4. **Exploratory Data Analysis (EDA)**:  
   - Aggregated metrics using Dask + DuckDB  
   - Most reported drugs, frequent reactions, and country-based trends  
   - Generated visualizations via Pandas and Matplotlib

---

## 📊 Key Visuals & Insights

- 📈 **Most Common Adverse Reactions** (Bar Chart)  
- 💊 **Top 10 Drugs with Most Reports** (Pie Chart)  
- 🌍 **Geographic Distribution of Reports**  
- ⏳ **Trend of Adverse Reports Over Time** (Line Chart)

---

## 🚧 Challenges & Fixes

| Challenge                             | Solution                                      |
|--------------------------------------|-----------------------------------------------|
| Corrupted / missing JSONs            | Error handling + logging invalid files        |
| Memory bottlenecks (16GB system RAM) | Dask partitioning + PyArrow streaming writes  |
| Deeply nested schema                 | Predefined flattening + JSON parsing pipeline |

---

## 📘 Lessons Learned

- Columnar formats like **Parquet** are game-changers for speed and size.
- **Dask + PyArrow** can outperform Spark for certain JSON workloads.
- Logging and validation are *critical* when handling messy open data.

---

## 🚀 Future Scope

> This was the **foundation**. A future version could evolve into:

- Real-time FDA event monitoring using **Kafka + Spark Streaming**
- Orchestrated pipelines using **Airflow**
- ML-based risk scoring for drugs with **scikit-learn or PyTorch**
- Fully cloud-deployable pipeline with **Azure or AWS**

Extended proposal that I will modify as it is tentative documentation:
📈 Next Steps to Upgrade Your FDA Drug Reaction Project
1. Structural Upgrades (Make it Real-World Scale)

Step	- Action	- Why It Matters
1.1	Dockerize your Spark + Kafka setup	- Reproducible, production-like local dev
1.2	Introduce Airflow DAGs	- True ETL orchestration, scheduling
1.3	Partition Parquet Lakehouse	Faster querying, better storage management (/year/month/)
1.4	Kafka Streaming ingestion	Move from batch → near real-time data pipelines
2. Data Engineering Upgrades

Step	- Action	- Why It Matters
2.1	Predefine Schema Mapping for JSON flattening -	Speed + memory efficiency
2.2	Incremental ingestion	- Don't reprocess entire dataset every time
2.3	Build metadata tracking (e.g., files processed logs) -	Debugging and audit trail
3. Analytics & ML Upgrades

Step	- Action	- Why It Matters
3.1	Set up DuckDB-powered interactive SQL (Notebook + CLI)	- Instant querying without Spark overhead
3.2	ML Model: Predict drug risk levels	- Major feature/pain point/cream of the crop
3.3	Anomaly Detection on severe adverse events	- Real-world use-case (early warning system)
4. Visualization and Dashboard Upgrades

Step	- Action	- Why It Matters
4.1	- Streamlit App for analytics	- Live interactive dashboard
4.2	- Superset or Metabase integration - 	SQL + visual exploration for stakeholders
4.3	- Add geographic mapping (Plotly or Folium) -	Location trends of adverse events
5. - Cloud and Scalability Upgrades

Step	- Action	- Why It Matters
5.1	  - Push Parquet data to Azure Blob or AWS S3 - Durable, scalable storage
5.2	  - Use Azure EventHub (or MSK) instead of local Kafka -	Managed streaming platform
5.3	  -  Spark on Databricks (optional) -	Insane speed + true scale-out
🧩 Visual Architecture Target
[OpenFDA JSONs]
    ↓
[Kafka Producer (streaming FDA events)]
    ↓
[Kafka Topic]
    ↓
[Spark Structured Streaming Consumer]
    ↓
[Transform & Flatten JSON]
    ↓
[Write to Parquet Lake (Azure / S3)]
    ↓
[Query via DuckDB / Spark SQL]
    ↓
[Airflow Orchestration for ETL + Analytics]
    ↓
[Streamlit Dashboard / Alerts]
🚀 Timeline Suggestion (If you want to split into phases)

Expected Phase	Focus
Week 1	Dockerize Spark + Kafka + Airflow locally
Week 2	Partitioned Parquet Lakehouse + DuckDB EDA
Week 3	Kafka streaming ingestion + Spark streaming ETL
Week 4	Build ML Risk Predictor + Anomaly Detection
Week 5	Cloud Storage (Azure or AWS) + Deploy dashboards
Week 6	Final Polishing: Documentation, diagrams, blog post
Keywords: Distributed ETL system, Real-time and batch processing pipelines, Lakehouse storage optimized for big data analytics, Live dashboard for stakeholders, ML-driven predictive insights, Cloud-scalable project portfolio


Docker-compose file with Spark + Kafka + Airflow ready

Airflow DAG template for ETL

Streamlit dashboard template for visualizations

