# Databricks notebook source
# MAGIC %run ./ingestion/etl_initialize_bronze

# COMMAND ----------

# MAGIC %run ./ingestion/etl_initialize_silver

# COMMAND ----------

# MAGIC %run ./features/initialize_features

# COMMAND ----------

# MAGIC %run ./inference/initialize_gold

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
