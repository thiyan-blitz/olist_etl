import csv
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

DB = dict(host="warehouse", port=5432, dbname="olist",
          user="warehouse", password="warehouse")

DATA_DIR = "/opt/airflow/data"
FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
}


def load_csv_to_staging(table: str, filename: str):
    path = f"{DATA_DIR}/{filename}"

    # read the header so we can build the table from the file itself
    with open(path, encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))

    cols = ", ".join(f'"{c}" TEXT' for c in header)

    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS staging.{table} ({cols});")
            cur.execute(f"TRUNCATE staging.{table};")   # makes reruns idempotent
            with open(path, encoding="utf-8") as f:
                cur.copy_expert(
                    f"COPY staging.{table} FROM STDIN WITH CSV HEADER", f
                )
            cur.execute(f"SELECT COUNT(*) FROM staging.{table};")
            count = cur.fetchone()[0]
        conn.commit()
        print(f"Loaded {count} rows into staging.{table}")
    finally:
        conn.close()

def run_transform():
    with open("/opt/airflow/sql/transform.sql") as f:
        sql = f.read()
    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Transform complete")
    finally:
        conn.close()
def run_quality_checks():
    # (description, SQL returning a count of bad rows, must be 0)
    hard_checks = [
        ("fact rows match staging order_items",
         """SELECT ABS((SELECT COUNT(*) FROM dw.fact_order_items)
                     - (SELECT COUNT(*) FROM staging.order_items))"""),
        ("no null keys in fact",
         """SELECT COUNT(*) FROM dw.fact_order_items
            WHERE customer_id IS NULL OR product_id IS NULL OR date_key IS NULL"""),
        ("no orphan customers in fact",
         """SELECT COUNT(*) FROM dw.fact_order_items f
            LEFT JOIN dw.dim_customer c ON c.customer_id = f.customer_id
            WHERE c.customer_id IS NULL"""),
        ("no orphan products in fact",
         """SELECT COUNT(*) FROM dw.fact_order_items f
            LEFT JOIN dw.dim_product p ON p.product_id = f.product_id
            WHERE p.product_id IS NULL"""),
        ("no negative prices",
         "SELECT COUNT(*) FROM dw.fact_order_items WHERE price < 0"),
    ]

    conn = psycopg2.connect(**DB)
    failures = []
    try:
        with conn.cursor() as cur:
            for name, sql in hard_checks:
                cur.execute(sql)
                bad = cur.fetchone()[0]
                status = "PASS" if bad == 0 else "FAIL"
                print(f"[{status}] {name} (bad rows: {bad})")
                if bad != 0:
                    failures.append(name)

            # soft check: warn only, don't fail the pipeline
            cur.execute("""SELECT COUNT(*) FROM dw.fact_order_items
                           WHERE delivered_at IS NOT NULL
                             AND delivered_at < purchased_at""")
            odd = cur.fetchone()[0]
            if odd:
                print(f"[WARN] {odd} items delivered before purchase time")
    finally:
        conn.close()

    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")
    print("All data quality checks passed")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="olist_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    load_tasks = []
    for table, filename in FILES.items():
        load_tasks.append(
            PythonOperator(
                task_id=f"load_{table}",
                python_callable=load_csv_to_staging,
                op_kwargs={"table": table, "filename": filename},
            )
        )

    transform = PythonOperator(task_id="transform_star_schema",
                               python_callable=run_transform)
    quality = PythonOperator(task_id="data_quality_checks",
                             python_callable=run_quality_checks)

    load_tasks >> transform >> quality