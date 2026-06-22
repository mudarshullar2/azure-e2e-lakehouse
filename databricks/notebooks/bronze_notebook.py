# Databricks notebook source
# reading the data from the landing zone and creating a delta table 
df = (spark.read.format("parquet")
      .option("inferSchema", True)
      .load("abfss://landing-zone@lakehouseadlsms01.dfs.core.windows.net/raw_data/"))

df.write.format("delta").mode("overwrite").saveAsTable("cars_catalog.bronze.sales")

# COMMAND ----------

