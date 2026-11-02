# Olist E-Commerce Batch ETL and Data Warehouse

An Airflow-orchestrated batch pipeline that loads the Olist Brazilian
e-commerce dataset into PostgreSQL, transforms it into a star schema, and
validates it with automated data quality checks.

## Architecture

```
  CSV files (4)
       |
       v
+---------------+     +----------------------+     +-------------------+
| load_* tasks  | --> | transform_star_schema| --> | data_quality_checks|
| (4 parallel)  |     | staging -> dw        |     | fail DAG on bad data|
+---------------+     +----------------------+     +-------------------+
   staging schema          dw schema (star)

            dim_customer   dim_product   dim_date
                   \           |           /
                    +-- fact_order_items --+
```

## Data model
- **Grain of the fact table:** one row per order item.
- **fact_order_items:** price, freight_value, order_status, purchase and
  delivery timestamps.
- **Dimensions:** dim_customer, dim_product, dim_date.
- Olist generates a new `customer_id` per order, so `dim_customer` also keeps
  `customer_unique_id` to identify real people across orders.

## Key design decisions
- **Staging layer:** raw CSVs land as text, untouched. Type casting happens in
  the transform so bad values never break the load.
- **Idempotent runs:** loads use TRUNCATE + COPY and the transform drops and
  rebuilds the warehouse tables, so reruns never duplicate data.
- **Task dependencies:** the 4 loads run in parallel, and the transform only
  starts if all of them succeed.
- **Data quality gate:** checks for row-count reconciliation, null keys,
  orphan foreign keys, and negative prices. Hard failures stop the pipeline;
  soft anomalies (delivered before purchase) only log a warning.
- **Retries:** every task retries twice before failing.

## Results
- Loaded 99,441 orders, 112,650 order items, 99,441 customers, 32,951 products.
- Fact table reconciles exactly with staging (112,650 rows).
- Verified the quality checks by deliberately deleting a dimension row; the
  orphan-key check failed and blocked the pipeline.

## How to run
```bash
# put the 4 Olist CSVs in ./data
docker compose up -d
docker exec airflow airflow dags unpause olist_etl
docker exec airflow airflow dags trigger olist_etl
```
Airflow UI: http://localhost:8080

## What I'd do at 100x scale
- Land raw files in S3 as Parquet, partitioned by date, and load to Redshift
  with COPY.
- Make loads incremental (by order date) instead of full reloads.
- Use dbt for transforms and tests, and Great Expectations for validation.
- Add alerting (Slack/email) on failures and track check results over time.
- Use SCD Type 2 for dimensions that change, such as customer address.