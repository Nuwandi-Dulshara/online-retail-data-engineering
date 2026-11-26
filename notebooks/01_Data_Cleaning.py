# Databricks notebook source
# DBTITLE 1,Load dataset
# Load the Bronze layer dataset
df = spark.table("workspace.default.online_retail")

# Display the first 10 records
display(df.limit(10))

# COMMAND ----------

# Total number of records
print("Total Records:", df.count())

# Total number of columns
print("Total Columns:", len(df.columns))

# Display column names
print("Columns:", df.columns)

# Display data types
df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# Count missing values in each column
display(
    df.select([
        F.count(
            F.when(
                F.col(c).isNull() |
                (F.trim(F.col(c).cast("string")) == ""),
                c
            )
        ).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

df_clean = df.toDF(
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country"
)

display(df_clean.limit(10))

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn(
        "invoice_no",
        F.upper(F.trim(F.col("invoice_no").cast("string")))
    )
    .withColumn(
        "stock_code",
        F.upper(F.trim(F.col("stock_code").cast("string")))
    )
    .withColumn(
        "description",
        F.trim(F.col("description"))
    )
    .withColumn(
        "country",
        F.trim(F.col("country"))
    )
)

display(df_clean.limit(10))

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(18,4))"))
    .withColumn("customer_id", F.expr("try_cast(customer_id AS BIGINT)"))
    .withColumn(
        "invoice_date",
        F.coalesce(
            F.expr("try_cast(invoice_date AS TIMESTAMP)"),
            F.try_to_timestamp(
                F.col("invoice_date"),
                F.lit("M/d/yyyy H:mm")
            ),
            F.try_to_timestamp(
                F.col("invoice_date"),
                F.lit("M/d/yyyy H:mm:ss")
            )
        )
    )
)

df_clean.printSchema()

# COMMAND ----------

display(
    df_clean.select("invoice_date").limit(20)
)

# COMMAND ----------

df_clean = df_clean.dropna(
    subset=[
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "unit_price",
        "invoice_date",
        "country"
    ]
)

df_clean = df_clean.filter(
    (F.col("invoice_no") != "") &
    (F.col("stock_code") != "") &
    (F.col("description") != "") &
    (F.col("country") != "")
)

print("Remaining records:", df_clean.count())

# COMMAND ----------

before_duplicates = df_clean.count()

after_duplicates = df_clean.dropDuplicates().count()

print(
    "Duplicate Records:",
    before_duplicates - after_duplicates
)

# COMMAND ----------

df_clean = df_clean.dropDuplicates()

print("Records after removing duplicates:",
      df_clean.count())

# COMMAND ----------

cancelled_transactions = df_clean.filter(
    F.col("invoice_no").startswith("C")
)

print("Cancelled transaction records:",
      cancelled_transactions.count())

display(cancelled_transactions.limit(10))

# COMMAND ----------

df_clean = df_clean.filter(
    ~F.col("invoice_no").startswith("C")
)

# COMMAND ----------

df_clean = df_clean.filter(
    (F.col("quantity") > 0) &
    (F.col("unit_price") > 0)
)

display(df_clean.limit(10))

# COMMAND ----------

df_clean = df_clean.withColumn(
    "total_amount",
    F.round(
        F.col("quantity") * F.col("unit_price"),
        4
    )
)

display(
    df_clean.select(
        "invoice_no",
        "description",
        "quantity",
        "unit_price",
        "total_amount"
    ).limit(10)
)

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn("year", F.year("invoice_date"))
    .withColumn("month", F.month("invoice_date"))
    .withColumn("month_name", F.date_format("invoice_date", "MMMM"))
    .withColumn("day", F.dayofmonth("invoice_date"))
    .withColumn("day_name", F.date_format("invoice_date", "EEEE"))
    .withColumn("hour", F.hour("invoice_date"))
    .withColumn("order_date", F.to_date("invoice_date"))
)

display(df_clean.limit(10))

# COMMAND ----------

df_clean = df_clean.withColumn(
    "processed_at",
    F.current_timestamp()
)

display(df_clean.limit(10))

# COMMAND ----------

print("Original Records:", df.count())

print("Cleaned Records:", df_clean.count())

print("Removed Records:",
      df.count() - df_clean.count())

print("Total Columns:", len(df_clean.columns))

# COMMAND ----------

display(
    df_clean.select([
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in df_clean.columns
    ])
)

# COMMAND ----------

invalid_quantity = df_clean.filter(
    F.col("quantity") <= 0
).count()

invalid_price = df_clean.filter(
    F.col("unit_price") <= 0
).count()

cancelled_orders = df_clean.filter(
    F.col("invoice_no").startswith("C")
).count()

missing_dates = df_clean.filter(
    F.col("invoice_date").isNull()
).count()

print("Invalid Quantities:", invalid_quantity)
print("Invalid Prices:", invalid_price)
print("Cancelled Orders:", cancelled_orders)
print("Missing Dates:", missing_dates)

# COMMAND ----------

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.online_retail_silver")

# COMMAND ----------

print("Silver layer table created successfully!")

# COMMAND ----------

silver_df = spark.table(
    "workspace.default.online_retail_silver"
)

display(silver_df.limit(20))

# COMMAND ----------

print("Silver Records:", silver_df.count())

silver_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.default.online_retail_silver
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(SUM(total_amount), 2) AS total_sales
# MAGIC FROM workspace.default.online_retail_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT invoice_no) AS total_orders,
# MAGIC     COUNT(DISTINCT customer_id) AS total_customers,
# MAGIC     SUM(quantity) AS total_products_sold
# MAGIC FROM workspace.default.online_retail_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     country,
# MAGIC     ROUND(SUM(total_amount), 2) AS total_sales
# MAGIC FROM workspace.default.online_retail_silver
# MAGIC GROUP BY country
# MAGIC ORDER BY total_sales DESC
# MAGIC LIMIT 10;