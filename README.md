# Olist E-Commerce Data Engineering Pipeline

An end-to-end data engineering project built around the Olist Brazilian E-Commerce dataset, implementing a scalable ETL workflow using **PySpark, Python, and Parquet**. The project follows the Medallion Architecture (Bronze, Silver, Gold) to transform raw transactional data into structured, analytics-ready datasets using data quality validation and dimensional modeling.

## Project Overview

The project processes data from the Olist Brazilian E-Commerce platform, integrating multiple interconnected datasets covering customers, orders, products, sellers, payments, reviews, and geographical information.

The primary objective is to build a structured data pipeline that transforms raw transactional data into reliable analytical datasets through three processing layers:

- **Bronze:** Ingest and preserve raw source data in Parquet format.
- **Silver:** Clean, standardize, transform, and validate datasets.
- **Gold:** Build fact and dimension tables using a Star Schema for analytical workloads.

## Architecture

```text
                 Olist E-Commerce Dataset
                    (9 CSV Files)
                          |
                          v
                 Data Ingestion Layer
                          |
                          v
                 BRONZE LAYER
              Raw Data -> Parquet
              Source Metadata
                          |
                          v
                 SILVER LAYER
              Data Cleaning
              Schema Enforcement
              Type Conversion
              Deduplication
              Data Quality Checks
                          |
                          v
                  GOLD LAYER
               Dimensional Modeling
                  Star Schema
                          |
                 +--------+--------+
                 |                 |
                 v                 v
           Fact Tables       Dimension Tables
                 |                 |
                 +--------+--------+
                          |
                          v
                 PostgreSQL Warehouse
                          |
                          v
                  Power BI Dashboard
```

*PostgreSQL and Power BI represent planned downstream integrations.*

## Technology Stack

| Category | Technologies |
|---|---|
| Programming Language | Python |
| Data Processing | Apache Spark, PySpark |
| Data Manipulation | Pandas, NumPy |
| Storage Format | Apache Parquet |
| Data Architecture | Medallion Architecture |
| Data Modeling | Star Schema |
| Development Environment | Google Colab, Jupyter Notebook |
| Version Control | Git, GitHub |
| Planned Warehouse | PostgreSQL |
| Planned Visualization | Power BI |
| Planned Cloud Integration | AWS S3, Lambda, Step Functions, CloudWatch |

## Dataset Information

**Dataset:** Brazilian E-Commerce Public Dataset by Olist

**Source:** [Olist E-Commerce Dataset - Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

The dataset consists of nine interconnected CSV files:

| Dataset | Description | Records |
|---|---|---:|
| Customers | Customer information and location | 99,441 |
| Geolocation | Brazilian postal code coordinates | 1,000,163 |
| Orders | Order details and delivery timestamps | 99,441 |
| Order Items | Products associated with each order | 112,650 |
| Payments | Payment methods and transaction values | 103,886 |
| Reviews | Customer reviews and ratings | 99,224 |
| Products | Product attributes and categories | 32,951 |
| Sellers | Seller information and location | 3,095 |
| Category Translation | Portuguese-to-English category mapping | 71 |
| **Total** | **9 datasets** | **1,550,922** |

## Medallion Architecture Implementation

### 1. Bronze Layer - Raw Data Ingestion

The Bronze layer serves as the initial landing zone for source datasets.

**Implementation:**
- Ingested all nine CSV datasets using PySpark.
- Preserved original source values by initially reading columns as strings.
- Converted CSV datasets into Apache Parquet format.
- Added ingestion metadata to maintain source traceability.

**Metadata columns:**
- `_source_file`
- `_ingested_at_utc`
- `_bronze_table`

**Key objective:** Preserve source data while establishing a structured and traceable foundation for downstream transformations.

### 2. Silver Layer - Data Cleaning and Transformation

The Silver layer focuses on improving data consistency and reliability.

**Transformations performed:**
- Schema standardization and explicit data type conversion.
- Whitespace trimming and empty-string normalization.
- Conversion of empty values into NULL.
- Duplicate detection and removal of exact duplicate geolocation records.
- Timestamp and numerical field standardization.
- Primary-key quality checks.
- Foreign-key and referential integrity validation.

**Data Quality Highlights:**

| Metric | Result |
|---|---:|
| Geolocation records profiled | 1,000,163 |
| Exact duplicate records identified | 261,831 |
| Duplicate reduction | 26.2% |
| Records retained after deduplication | 738,332 |

The transformation process also preserves legitimate missing values, such as delivery timestamps for orders that have not been delivered, rather than applying arbitrary imputation or deletion.

### 3. Gold Layer - Dimensional Modeling

The Gold layer transforms cleaned datasets into analytics-ready dimensional tables.

The planned dimensional model follows a **Star Schema** consisting of three fact tables and six dimension tables.

**Fact Tables**

| Table | Grain | Description |
|---|---|---|
| fact_order_items | Order ID + Order Item ID | Product-level order transactions, pricing, and freight |
| fact_payments | Order ID + Payment Sequential | Payment values, installments, and payment methods |
| fact_reviews | Review business record | Customer ratings and review information |

**Dimension Tables**

| Table | Description |
|---|---|
| dim_customer | Customer attributes and location |
| dim_product | Product attributes and translated categories |
| dim_seller | Seller information and location |
| dim_date | Calendar attributes for analytical date filtering |
| dim_order_status | Order lifecycle and status information |
| dim_payment_type | Payment method classifications |

**Modeling techniques:**
- Surrogate key generation for dimension tables.
- Fact-to-dimension relationships using dimension keys.
- Left joins to preserve fact records.
- Deterministic review record keys.
- Fact grain duplicate validation.
- Foreign-key integrity audits.

## Data Quality and Validation

Data quality checks are incorporated throughout the ETL workflow.

| Validation | Purpose |
|---|---|
| Record Count Reconciliation | Verify source-to-layer record preservation |
| Schema Validation | Ensure expected columns and data types |
| Null Profiling | Identify missing and incomplete values |
| Duplicate Detection | Identify repeated records and keys |
| Primary-Key Validation | Check key uniqueness and null values |
| Referential Integrity | Validate relationships between datasets |
| Fact Grain Validation | Prevent accidental row multiplication |
| Foreign-Key Auditing | Identify unmatched dimension references |

The pipeline maintains audit reports to document data quality findings and transformation outcomes.

## Project Structure

```text
ecommerce-data-engineering/
|
+-- data/
|   +-- raw/
|   |   +-- olist_customers_dataset.csv
|   |   +-- olist_orders_dataset.csv
|   |   +-- olist_order_items_dataset.csv
|   |   +-- olist_order_payments_dataset.csv
|   |   +-- olist_order_reviews_dataset.csv
|   |   +-- olist_products_dataset.csv
|   |   +-- olist_sellers_dataset.csv
|   |   +-- olist_geolocation_dataset.csv
|   |   +-- product_category_name_translation.csv
|   |
|   +-- bronze/
|   +-- silver/
|   +-- gold/
|
+-- notebooks/
|   +-- 01_data_ingestion.ipynb
|   +-- 02_data_profiling.ipynb
|   +-- 03_bronze_layer.ipynb
|   +-- 04_silver_transformations.ipynb
|   +-- 05_gold_dimensional_model.ipynb
|   +-- 06_postgresql_warehouse.ipynb
|
+-- reports/
|   +-- source_inventory.csv
|   +-- table_profiles.csv
|   +-- key_quality_report.csv
|   +-- null_profile.csv
|   +-- relationship_report.csv
|   +-- gold_table_audit.csv
|   +-- gold_foreign_key_audit.csv
|
+-- README.md
+-- .gitignore
```

*The structure represents the intended project organization; include only notebooks and reports that have actually been created.*

## Getting Started

### Prerequisites

- Python 3.10+
- Google account with Google Colab access
- Google Drive
- Olist dataset downloaded from Kaggle

### 1. Clone the Repository

```bash
git clone https://github.com/Muktesh9-11/olist-data-engineering.git

cd olist-data-engineering
```

### 2. Download the Dataset

Download the Olist Brazilian E-Commerce dataset from Kaggle and extract the nine CSV files.

Place the extracted files inside:

```text
data/raw/
```

### 3. Open Google Colab

Upload the project notebooks to Google Colab and mount Google Drive:

```python
from google.colab import drive
drive.mount('/content/drive')
```

### 4. Install Dependencies

```python
!pip install pyspark pandas numpy pyarrow
```

### 5. Configure the Project Path

```python
from pathlib import Path

PROJECT = Path(
    "/content/drive/MyDrive/ecommerce-data-engineering"
)

RAW = PROJECT / "data" / "raw"
BRONZE = PROJECT / "data" / "bronze"
SILVER = PROJECT / "data" / "silver"
GOLD = PROJECT / "data" / "gold"
REPORTS = PROJECT / "reports"
```

### 6. Execute the Pipeline

Execute the notebooks sequentially:

1. Data ingestion and dataset setup.
2. Exploratory data analysis and source profiling.
3. Bronze layer ingestion.
4. Silver layer transformations.
5. Gold layer dimensional modeling.

Each layer reads the persisted Parquet datasets from the previous layer.

## Future Enhancements

The following components are planned extensions to evolve the project into a cloud-based data engineering solution.

### PostgreSQL Data Warehouse
- Load Gold fact and dimension tables into PostgreSQL.
- Implement relational constraints and indexing.
- Develop analytical SQL queries for business reporting.

### Power BI Dashboard
Build interactive dashboards to analyze:
- Revenue and order trends.
- Customer purchasing behavior.
- Product category performance.
- Seller performance.
- Payment distribution.
- Delivery delays and customer reviews.

### AWS Cloud Integration
Extend the pipeline using AWS services:

| AWS Service | Intended Purpose |
|---|---|
| Amazon S3 | Cloud storage for Bronze, Silver, and Gold datasets |
| AWS Lambda | Lightweight event-driven automation |
| AWS Step Functions | Pipeline workflow orchestration |
| Amazon CloudWatch | Logging, monitoring, and alerting |

The cloud deployment will be implemented after validating the local ETL pipeline.

## Key Learning Outcomes

- Designing and implementing Medallion Architecture.
- Processing interconnected datasets using PySpark.
- Working with distributed data processing concepts.
- Building reliable ETL workflows.
- Implementing data quality and integrity checks.
- Converting transactional datasets into dimensional models.
- Designing analytics-ready fact and dimension tables.
- Preparing data for SQL-based warehousing and business intelligence.

## Author

**Muktesh Bodkhe**

- GitHub: [Muktesh9-11](https://github.com/Muktesh9-11)
- LinkedIn: [Muktesh Bodkhe](https://www.linkedin.com/in/muktesh-bodkhe-9335a8285/)

---
