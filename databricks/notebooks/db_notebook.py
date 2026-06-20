# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog cars_catalog;

# COMMAND ----------

# creating schema for silver and gold layer

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold

# COMMAND ----------
