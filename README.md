The purpose of this project is to create a modern analytics pipeline.

Apache Airflow orchestrates the pipeline. It ingests raw data from open-meteo, a weather API, transforms the data with a DAG written in python, stores the analytics data in ClickHouse, and then visualizes it with Metabase.

The major goals were to demonstrate:
production-style orchestration with Airflow.
OLAP-optimized storage with ClickHouse.
End to end data visibility (DAG -> database -> dashboard).

Project Demonstrates
End to end ELT pipeline design
clear separation of orchestration, storage, and visualization
OLAP-appropriate database choices
Dockerized local dev environment
Production style observability

Architectural overview:
Source Data
↓
Airflow DAG (Extract → Transform → Load)
↓
ClickHouse (Analytics Tables)
↓
Metabase (Exploration & Dashboards)

(I will (hopefully) add an architecture diagram, time permitting.)

Technologies Used:
Apache Airflow – workflow orchestration and scheduling

ClickHouse – column-oriented OLAP database

Metabase – analytics UI and dashboards

Docker / Docker Compose – containerization

Python – DAGs and data processing

SQL – analytics queries

Airflow pipeline orchestration
The airflow DAG:

1. Extracts source data from open-meteo
2. Applies transformation (currently, basic metadata injection)
3. Loads results into ClickHouse table
   (airflow screnshot coming soon)

ClickHouse analytics storage
Processed data is written to CLickHouse table which has been optimized for analytical queries.
(ClickHouse example query and query result screenshots coming soon)

Metabase exploration and dashboards
Metabase connects directly to ClickHouse to to provide:
table exploration
ad-hoc/on demand queries
dashboard for key metrics.
(metabase screenshots coming soon)

Future improvements
add dbt transformations for analytics modeling
implement incremental loading strategies
add data quality checks and airflow sensors
expand metabase dashboards for increased business metric visibility
deploy to managed airflow/ClickHouse envs

Difficulties overcome:

Infra & orchestration
Airflow PID conflicts
Gunicorn startup timing
Container lifecycle confusion
Volume-mounted state persistence

Networking
Service name resolution (clickhouse)
DNS flakiness inside Docker
External API availability

ClickHouse specifics
HTTP auth
Query formatting
Insert semantics
Error codes that aren’t obvious

Mental overhead
Many moving pieces at once
Unclear failure attribution
Knowing when not to panic
