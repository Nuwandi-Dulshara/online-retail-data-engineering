-- Databricks notebook source
SELECT *
FROM workspace.default.online_retail_silver
LIMIT 10;

-- COMMAND ----------

SELECT COUNT(*) AS total_records
FROM workspace.default.online_retail_silver;

-- COMMAND ----------

DESCRIBE TABLE workspace.default.online_retail_silver;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_sales_summary
USING DELTA
AS
SELECT
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT stock_code) AS total_products,
    COUNT(DISTINCT country) AS total_countries,
    ROUND(
        SUM(total_amount) / COUNT(DISTINCT invoice_no),
        2
    ) AS average_order_value
FROM workspace.default.online_retail_silver;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_sales_summary;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_daily_sales
USING DELTA
AS
SELECT
    order_date,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT customer_id) AS total_customers
FROM workspace.default.online_retail_silver
GROUP BY order_date;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_daily_sales
ORDER BY order_date;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_monthly_sales
USING DELTA
AS
SELECT
    year,
    month,
    month_name,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT customer_id) AS total_customers
FROM workspace.default.online_retail_silver
GROUP BY year, month, month_name;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_monthly_sales
ORDER BY year, month;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_country_sales
USING DELTA
AS
SELECT
    country,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(quantity) AS total_units_sold
FROM workspace.default.online_retail_silver
GROUP BY country;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_country_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_product_sales
USING DELTA
AS
SELECT
    stock_code,
    MAX(description) AS description,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    ROUND(AVG(unit_price), 2) AS average_unit_price
FROM workspace.default.online_retail_silver
GROUP BY stock_code;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_product_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_product_sales
ORDER BY total_units_sold DESC
LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_customer_sales
USING DELTA
AS
SELECT
    customer_id,
    COUNT(DISTINCT invoice_no) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_spent,
    SUM(quantity) AS total_units_purchased,
    MIN(order_date) AS first_purchase,
    MAX(order_date) AS last_purchase,
    ROUND(
        SUM(total_amount) / COUNT(DISTINCT invoice_no),
        2
    ) AS average_order_value
FROM workspace.default.online_retail_silver
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_customer_sales
ORDER BY total_spent DESC
LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_customer_segments
USING DELTA
AS
SELECT
    *,
    CASE
        WHEN total_spent >= 5000 THEN 'High Value'
        WHEN total_spent >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM workspace.default.gold_customer_sales;

-- COMMAND ----------

SELECT
    customer_segment,
    COUNT(*) AS total_customers,
    ROUND(SUM(total_spent), 2) AS total_revenue
FROM workspace.default.gold_customer_segments
GROUP BY customer_segment;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_hourly_sales
USING DELTA
AS
SELECT
    hour,
    COUNT(DISTINCT invoice_no) AS total_orders,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    SUM(quantity) AS total_units_sold
FROM workspace.default.online_retail_silver
GROUP BY hour;

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_hourly_sales
ORDER BY hour;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_weekly_sales
USING DELTA
AS
SELECT
    CAST(DATE_TRUNC('WEEK', order_date) AS DATE) AS week_start,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(quantity) AS total_units_sold
FROM workspace.default.online_retail_silver
GROUP BY CAST(DATE_TRUNC('WEEK', order_date) AS DATE);

-- COMMAND ----------

SELECT *
FROM workspace.default.gold_weekly_sales
ORDER BY week_start;

-- COMMAND ----------

SHOW TABLES IN workspace.default;

-- COMMAND ----------

SELECT
    (
        SELECT ROUND(SUM(total_amount), 2)
        FROM workspace.default.online_retail_silver
    ) AS silver_revenue,
    (
        SELECT total_revenue
        FROM workspace.default.gold_sales_summary
    ) AS gold_revenue;

-- COMMAND ----------

