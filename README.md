# Ecommerce Data Pipeline

An end-to-end, production-grade Data Engineering pipeline built with Python and AWS.
Implements the **Medallion Architecture** (Bronze → Silver → Gold) with real-time
streaming, batch ETL, data quality, a star-schema warehouse, dbt transformations,
and analytics dashboards.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                                     │
│  PostgreSQL / RDS            Kafka (MSK)           External APIs            │
│  (orders, users, products)   (user events)         (optional)               │
└───────────┬──────────────────────┬─────────────────────────────────────────┘
            │  Batch (daily)       │  Streaming (real-time)
            ▼                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER  (S3 / MinIO)                          │
│  Raw JSON, date-partitioned, immutable, schema-on-read                    │
│  s3://bucket/bronze/{table}/year=YYYY/month=MM/day=DD/                    │
└───────────────────────────────┬───────────────────────────────────────────┘
                                │  PySpark
                                ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER  (S3 / MinIO)                          │
│  Cleaned Parquet, typed, deduplicated, validated                          │
│  s3://bucket/silver/{entity}/  (partitioned by date)                      │
└──────────────┬──────────────────────────┬────────────────────────────────┘
               │  Great Expectations      │  PySpark
               ▼                          ▼
┌──────────────────────────┐  ┌──────────────────────────────────────────────┐
│   DATA QUALITY CHECKS    │  │              GOLD LAYER  (S3)                │
│   - Uniqueness           │  │  Business aggregates (Parquet)               │
│   - Null rates           │  │  daily_revenue / product_metrics /           │
│   - Value ranges         │  │  user_order_summary                          │
│   - Referential checks   │  └────────────────────┬─────────────────────────┘
└──────────────────────────┘                       │  Redshift COPY / awswrangler
                                                   ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                    WAREHOUSE  (Redshift / PostgreSQL)                     │
│  staging.*  →  dwh.* (Star Schema)  →  analytics.* (Views)               │
│  fact_orders / dim_users / dim_products / dim_date                        │
└──────────────────────────────┬────────────────────────────────────────────┘
                               │  dbt
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         DBT TRANSFORMATIONS                               │
│  staging/  →  marts/finance/  →  marts/marketing/  →  marts/product/    │
│  revenue_daily / customer_ltv / product_performance                      │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                       ANALYTICS & DASHBOARDS                              │
│  Grafana  |  Python Reports  |  SQL Analytics Queries                    │
└──────────────────────────────────────────────────────────────────────────┘

        Orchestration: Apache Airflow     Infrastructure: Terraform (AWS)
        CI/CD: GitHub Actions             Monitoring: CloudWatch + Grafana
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion (Batch) | Python, psycopg2, boto3, pandas |
| Ingestion (Streaming) | Apache Kafka, confluent-kafka |
| Processing | Apache PySpark 3.5 |
| Data Lake | AWS S3 / MinIO (local) |
| Orchestration | Apache Airflow 2.8 |
| Data Quality | Great Expectations (custom validators) |
| Warehouse | Amazon Redshift Serverless / PostgreSQL (local) |
| Transformations | dbt-core 1.7 |
| Analytics | SQL, pandas, Grafana |
| Infrastructure | Terraform, AWS (S3, MSK, Redshift, Glue, IAM) |
| CI/CD | GitHub Actions |

---

## Data Engineering Concepts Covered

- **Medallion Architecture** — Bronze / Silver / Gold layering
- **Batch ETL** — Incremental extraction with watermark-based partitioning
- **Stream Ingestion** — Kafka producer/consumer with micro-batch S3 sink
- **PySpark Transformations** — Schema enforcement, deduplication, window functions
- **Star Schema Design** — Fact tables + SCD-ready dimensions
- **Data Quality** — Automated checks with pass/fail pipeline gates
- **dbt** — Staging → Intermediate → Mart model layers with tests
- **Orchestration** — Airflow DAGs with retry logic and task dependencies
- **Infrastructure as Code** — Terraform for all AWS resources
- **Data Catalog** — AWS Glue crawler auto-discovers Silver/Gold schemas
- **Analytics** — CLV, cohort retention, revenue trends, funnel analysis
- **CI/CD** — Lint, unit tests, dbt compile check, Terraform validate on every PR

---

## Quick Start (Local)

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Make

### 1. Clone & setup
```bash
git clone https://github.com/iamkasha/ecommerce-data-pipeline
cd ecommerce-data-pipeline
cp .env.example .env
pip install -r requirements.txt
```

### 2. Start all services
```bash
make up
```

Services available:
| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8090 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |

### 3. Seed source data
```bash
make seed
# Generates: 2,000 users, 300 products, 10,000 orders
```

### 4. Run the full pipeline
```bash
make run-pipeline
```

Or trigger from Airflow UI → DAG: `batch_ingestion`

### 5. Start streaming
```bash
make stream-start
# Open Kafka UI to watch events flowing into ecom.user_events
```

### 6. View analytics
```bash
make analytics
```

---

## Project Structure

```
ecommerce-data-pipeline/
├── ingestion/
│   ├── batch/
│   │   └── postgres_extractor.py   # PostgreSQL → Bronze S3
│   └── streaming/
│       ├── kafka_producer.py       # Simulates user events
│       └── kafka_consumer.py       # Events → Bronze S3
├── processing/
│   ├── bronze_to_silver/
│   │   ├── clean_orders.py         # PySpark: clean + validate
│   │   └── clean_users.py          # PySpark: users + products
│   └── silver_to_gold/
│       └── aggregate_daily.py      # PySpark: business aggregates
├── dbt/
│   └── models/
│       ├── staging/                # Typed views from staging schema
│       ├── intermediate/           # Reusable business logic
│       └── marts/
│           ├── finance/            # revenue_daily, fct_orders
│           ├── marketing/          # customer_ltv, cohort analysis
│           └── product/            # product_performance
├── data_quality/
│   └── validate.py                 # Great Expectations-style checks
├── warehouse/
│   ├── schemas/                    # DDL: staging, dwh, analytics
│   └── loaders/
│       └── redshift_loader.py      # Gold → Warehouse loader
├── airflow/
│   └── dags/
│       ├── batch_ingestion_dag.py  # Daily full pipeline DAG
│       └── streaming_pipeline_dag.py
├── analytics/
│   └── reports.py                  # Analytics report generator
├── terraform/                      # AWS Infrastructure as Code
├── .github/workflows/ci.yml        # GitHub Actions CI
└── docker-compose.yml
```

---

## AWS Production Deployment

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Fill in: vpc_id, subnet_ids, passwords

terraform init
terraform plan
terraform apply
```

Then update `.env` with real AWS endpoints and run:
```bash
ENVIRONMENT=prod python -m ingestion.batch.postgres_extractor
```

---

## Running Tests

```bash
make test
# Runs unit + integration tests with coverage report
```

