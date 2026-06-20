# Databricks notebook source
import display
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df = (
    spark.read.format("parquet")
    .option("inferSchema", True)
    .load("abfss://landing-zone@lakehouseadlsms01.dfs.core.windows.net/raw_data/")
)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# Data Transformation

# COMMAND ----------

# splitting the column Model_ID into an ID and a model category e.g. BMW-M1 -> M1 & BMW
from pyspark.sql import functions as F

df = df.withColumn("Model_Category", F.split(df["Model_ID"], "-")[0])

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df = df.withColumn("Sales_Revenues", F.col("Revenue") * F.col("Units_Sold"))

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import sum as F_sum

# total units sold per branch
df.groupBy("Year", "BranchName").agg(
    F_sum("Units_Sold").alias("Total_Units_Sold")
).sort("Year", "Total_Units_Sold", ascending=[True, False]).display()

# COMMAND ----------

# dealers that sold most units
df.groupBy("Year", "DealerName").agg(
    F_sum("Units_Sold").alias("Total_Units_Sold_Per_Dealer")
).sort("Year", "Total_Units_Sold_Per_Dealer", ascending=[True, False]).display()

# COMMAND ----------

# most sold models/product
df.groupBy("Year", "Product_Name").agg(
    F_sum("Units_Sold").alias("Most_Sold_Products")
).sort("Year", "Most_Sold_Products", ascending=[True, False]).display()

# COMMAND ----------

# dealers that made the most total revenues
df.groupBy("Year", "Product_Name").agg(
    F_sum("Sales_Revenues").alias("Most_Sales_Per_Dealer")
).sort("Year", "Most_Sales_Per_Dealer", ascending=[True, False]).display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("cars_catalog.silver.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.silver.sales limit 5

# COMMAND ----------
