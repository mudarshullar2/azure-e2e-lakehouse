# Databricks notebook source
df_gold = spark.read.table("cars_catalog.silver.sales")
display(df_gold.limit(5))

# COMMAND ----------

# renaming columns
df_gold = df_gold.withColumnRenamed("Branch_ID", "branch_id")
df_gold = df_gold.withColumnRenamed("Dealer_ID", "dealer_id")
df_gold = df_gold.withColumnRenamed("Model_ID", "model_id")
df_gold = df_gold.withColumnRenamed("Revenue", "unit_revenue")
df_gold = df_gold.withColumnRenamed("Units_Sold", "units_sold")
df_gold = df_gold.withColumnRenamed("Date_ID", "date_id")
df_gold = df_gold.withColumnRenamed("Day", "day")
df_gold = df_gold.withColumnRenamed("Month", "month")
df_gold = df_gold.withColumnRenamed("Year", "year")
df_gold = df_gold.withColumnRenamed("BranchName", "branch_name")
df_gold = df_gold.withColumnRenamed("DealerName", "dealer_name")
df_gold = df_gold.withColumnRenamed("Product_Name", "product_name")
df_gold = df_gold.withColumnRenamed("Model_Category", "model_category")
df_gold = df_gold.withColumnRenamed("Sales_Revenues", "total_revenue")

# COMMAND ----------

display(df_gold.limit(5))

# COMMAND ----------

pip install holidays

# COMMAND ----------

# creating the dimension tables
# date dimension table 
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import holidays

date_dim = (
    df_gold.select("date_id", "day", "month", "year").distinct()
    .withColumn("full_date", F.make_date("year", "month", "day"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("week", F.weekofyear("full_date"))
)

# e.g. in Berlin
de_berlin = holidays.Germany(state="BE", years=[2017, 2018, 2019, 2020])
holiday_rows = [(d.isoformat(), name) for d, name in de_berlin.items()]
holidays_df = spark.createDataFrame(holiday_rows, ["holiday_date", "holiday_name"]).withColumn("holiday_date", F.to_date("holiday_date"))

# surrogate key (date_key)
date_dim = date_dim.withColumn(
    "date_key",
    F.row_number().over(Window.orderBy("date_id"))
)

date_dim = (
    date_dim
    .join(holidays_df, date_dim.full_date == holidays_df.holiday_date, "left")
    .withColumn("is_holiday", F.col("holiday_name").isNotNull())
    .drop("holiday_date")
)

# COMMAND ----------

display(date_dim.limit(5))

# COMMAND ----------

# branch dimension table
branch_dim = df_gold.select("branch_id", "branch_name").distinct()

# surrogate key
branch_dim = branch_dim.withColumn(
    "branch_key",
    F.row_number().over(Window.orderBy("branch_id"))
)

display(branch_dim.limit(5))

# COMMAND ----------

# dealer dimension table
dealer_dim = df_gold.select("dealer_id", "dealer_name").distinct()
dealer_dim = dealer_dim.withColumn(
    "dealer_key",
    F.row_number().over(Window.orderBy("dealer_id"))
)
display(dealer_dim.limit(5))

# COMMAND ----------

# model dimension table
model_dim = df_gold.select("model_id", "model_category", "product_name").distinct()
model_dim = model_dim.withColumn(
    "model_key",
    F.row_number().over(Window.orderBy("model_id"))
)
display(model_dim.limit(5))

# COMMAND ----------

# sales fact table
sales_fact = (
    df_gold
    .join(date_dim, on="date_id", how="left")
    .join(branch_dim, on="branch_id", how="left")
    .join(dealer_dim, on="dealer_id", how="left")
    .join(model_dim, on="model_id", how="left")
    .select(
        "date_key", "branch_key", "dealer_key", "model_key",
        "units_sold", "unit_revenue", "total_revenue"
    )
)
display(sales_fact.limit(5))

# COMMAND ----------

# loading the tables into the gold layer using an incremental upsert
from delta.tables import DeltaTable

if spark.catalog.tableExists("cars_catalog.gold.branch_dimension"):
    target = DeltaTable.forName(spark, "cars_catalog.gold.branch_dimension")
    (
        target.alias("trg")
        .merge(branch_dim.alias("src"), "trg.branch_key = src.branch_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    branch_dim.write.format("delta").saveAsTable("cars_catalog.gold.branch_dimension")

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.date_dimension"):
    target = DeltaTable.forName(spark, "cars_catalog.gold.date_dimension")
    (
        target.alias("trg")
        .merge(date_dim.alias("src"), "trg.date_key = src.date_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    date_dim.write.format("delta").saveAsTable("cars_catalog.gold.date_dimension")

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.dealer_dimension"):
    target = DeltaTable.forName(spark, "cars_catalog.gold.dealer_dimension")
    (
        target.alias("trg")
        .merge(dealer_dim.alias("src"), "trg.dealer_key = src.dealer_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    dealer_dim.write.format("delta").saveAsTable("cars_catalog.gold.dealer_dimension")

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.model_dimension"):
    target = DeltaTable.forName(spark, "cars_catalog.gold.model_dimension")
    (
        target.alias("trg")
        .merge(model_dim.alias("src"), "trg.model_key = src.model_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    model_dim.write.format("delta").saveAsTable("cars_catalog.gold.model_dimension")

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.sales_fact"):
    target = DeltaTable.forName(spark, "cars_catalog.gold.sales_fact")
    (
        target.alias("trg")
        .merge(
            sales_fact.alias("src"),
            "trg.date_key = src.date_key and trg.branch_key = src.branch_key and trg.dealer_key = src.dealer_key and trg.model_key = src.model_key"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    sales_fact.write.format("delta").saveAsTable("cars_catalog.gold.sales_fact")

# COMMAND ----------

