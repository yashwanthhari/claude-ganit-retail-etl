# Ganit Inc. — what it does and what a data engineer there works with

Compiled on 12 Nov 2026 from public sources (listed at the bottom). Job ads and aggregator sites are
noisy, so treat this as a map for your first weeks, not a spec. Your team lead and project
onboarding doc are the real source of truth.

## The company in short

- AI and data analytics firm, founded 2017, headquartered in Chennai.
- Service lines: data engineering, analytics consulting / BI, AI-ML solutions, MLOps, cloud migration
  and modernization. Headcount figures differ a lot between sources, so don't rely on any of them.
- Partner status: AWS Advanced Tier Services Partner, and it works on the Databricks platform.
- Analyst positioning (AIM Research, 2026): "Seasoned" for Data Engineering Service Providers and for
  Databricks Services Partners; "Challenger" for AI services in Financial Services and in Life Sciences
  and Healthcare. Industries you may meet: retail/CPG, BFSI, life sciences/healthcare.
- It is a consultancy, so the stack depends on the **client's cloud**. Expect to be on Azure for one
  project and GCP or AWS for the next.

## Data-engineer work you can expect

Typical themes in Ganit's own descriptions and postings:

1. Migration and modernization: moving legacy systems and warehouses (SQL Server stored procedures,
   Informatica-style ETL) to cloud-native platforms.
2. Building data lakes and pipelines: ingestion, cleaning, transformation, consumption layers.
3. Cloud warehouse and lakehouse delivery (Databricks/Delta, Snowflake, BigQuery).
4. Feeding BI and ML: marts for dashboards, feature-ready datasets for data scientists.
5. Production hygiene: monitoring, spotting issues before they hit users, CI/CD, Agile delivery.

## Tech signals by cloud

Skills named in Ganit job listings, grouped (they come from different postings and projects):

| Flavour | Skills named |
|---|---|
| Azure | Azure Databricks, Azure Data Factory (ADF), SQL Server, Snowflake, Azure DevOps, PySpark, Python, SQL, Spotfire (as a "willing to learn" item) |
| GCP | BigQuery, Dataproc, Dataflow, Dataform, Looker/LookML, Spark, Scala/Java, Python, SQL, Git |
| General senior roles | Python + Spark + SQL, data modelling, mentoring, client-facing work, AWS/GCP/Azure data-engineer certification as a plus |
| Older/other | Informatica, AWS, data-warehouse ETL, Python/Scala for ETL |

## How this lab maps to that stack (all free, runs on a laptop)

| In a Ganit-style project | In this lab |
|---|---|
| ADLS Gen2 / S3 / GCS | local folders under `data/` |
| ADF metadata-driven copy / ingestion | `config/pipeline.yaml` + manifest of ingested files (`bronze.py`, `state.py`) |
| Databricks + PySpark + Delta Lake | local PySpark + `delta-spark` (`bronze.py`, `silver.py`, `gold.py`) |
| Bronze / Silver / Gold (medallion) | `data/lake/bronze`, `silver`, `gold` |
| MERGE / upserts, late-arriving data | `delta_utils.upsert` with a stale-update guard |
| Data quality (Great Expectations, dbt tests) | `dq.py` rules + quarantine tables + `marts.dq_summary` |
| Star schema for BI | `gold.py` (dims, fact, unknown-member row, surrogate keys) |
| Snowflake / BigQuery / Synapse | DuckDB (`warehouse.py`) |
| dbt / Dataform SQL models | versioned SQL in `sql/marts/` (exercise: port to dbt-duckdb) |
| ADF triggers / Airflow | `pipeline.py` CLI + optional `orchestration/airflow_dag.py` |
| Azure DevOps CI/CD | GitHub Actions (`.github/workflows/ci.yml`) |
| Legacy stored-procedure migration | `sql/legacy/sp_refresh_daily_sales.sql` (exercise 6) |

What this lab can't give you: managed-cloud things such as Unity Catalog, cluster sizing and cost,
IAM/networking, Key Vault. Read about those alongside it (exercise list covers what to look up).

## Questions worth asking in week one

- Which cloud and which warehouse/lakehouse is my project on?
- Is transformation done in Spark notebooks, dbt/Dataform, or stored procedures?
- How is code promoted (branches, PR review, environments, release pipeline)?
- Where do data-quality failures go, and who owns them?
- What is the client's SLA for freshness, and how is it monitored?

## Sources

- AIM Research profile of Ganit: https://aimresearch.co/companies/ganit
- Cutshort listings for Ganit Business Solutions: https://cutshort.io/company/ganit-business-solutions-46-nkC9zP6m and https://cutshort.io/a/ganitjobs-blog
- Foundit listing, Senior Data Engineer (Mumbai): https://www.foundit.in/job/senior-data-engineer-ganit-inc-mumbai-56265573
- Hirist listing, Senior Data Engineer Python/Spark: https://www.hirist.tech/j/senior-data-engineer-python-spark-1681968
- Ganit data engineering overview (third-party write-up): https://thetechmarketer.com/ganit-data-engineering-capabilities/
