-- Rebuilt from staging on every run, so reruns are idempotent.
DROP TABLE IF EXISTS dw.fact_order_items;
DROP TABLE IF EXISTS dw.dim_customer;
DROP TABLE IF EXISTS dw.dim_product;
DROP TABLE IF EXISTS dw.dim_date;

-- Customer dimension
CREATE TABLE dw.dim_customer AS
SELECT customer_id,
       customer_unique_id,
       customer_city,
       customer_state
FROM staging.customers;
ALTER TABLE dw.dim_customer ADD PRIMARY KEY (customer_id);

-- Product dimension
CREATE TABLE dw.dim_product AS
SELECT product_id,
       COALESCE(NULLIF(product_category_name, ''), 'unknown') AS category,
       NULLIF(product_weight_g, '')::NUMERIC AS weight_g,
       NULLIF(product_photos_qty, '')::INT   AS photos_qty
FROM staging.products;
ALTER TABLE dw.dim_product ADD PRIMARY KEY (product_id);

-- Date dimension (covers the range of purchase dates)
CREATE TABLE dw.dim_date AS
SELECT d::DATE                       AS date_key,
       EXTRACT(YEAR FROM d)::INT     AS year,
       EXTRACT(MONTH FROM d)::INT    AS month,
       EXTRACT(DAY FROM d)::INT      AS day,
       TO_CHAR(d, 'Day')             AS weekday,
       EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series(
        (SELECT MIN(order_purchase_timestamp::TIMESTAMP)::DATE FROM staging.orders),
        (SELECT MAX(order_purchase_timestamp::TIMESTAMP)::DATE FROM staging.orders),
        INTERVAL '1 day') AS d;
ALTER TABLE dw.dim_date ADD PRIMARY KEY (date_key);

-- Fact table: one row per order item
CREATE TABLE dw.fact_order_items AS
SELECT i.order_id,
       i.order_item_id::INT                          AS order_item_id,
       o.customer_id,
       i.product_id,
       o.order_purchase_timestamp::TIMESTAMP::DATE   AS date_key,
       o.order_status,
       i.price::NUMERIC(10,2)                        AS price,
       i.freight_value::NUMERIC(10,2)                AS freight_value,
       o.order_purchase_timestamp::TIMESTAMP         AS purchased_at,
       NULLIF(o.order_delivered_customer_date, '')::TIMESTAMP AS delivered_at,
       NULLIF(o.order_estimated_delivery_date, '')::TIMESTAMP AS estimated_delivery_at
FROM staging.order_items i
JOIN staging.orders o ON o.order_id = i.order_id;
ALTER TABLE dw.fact_order_items ADD PRIMARY KEY (order_id, order_item_id);