# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists cars_catalog

# COMMAND ----------

# creating schema for the three layers (bronze, silver and gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists cars_catalog.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists cars_catalog.gold

# MAGIC %sql
# MAGIC create external location if not exists landing_zone
# url 'abfss://landing-zone@lakehouseadlsms01.dfs.core.windows.net/raw_data/'
# with (storage credentials `lakehouse-dbw-ms01`)

# COMMAND ----------

