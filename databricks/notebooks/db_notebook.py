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

# COMMAND ----------

