# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### This Notebooks executes a streaming inference pipeline. It ingests claims data, builds features and performs inference using the model fitted and registered in Fitting Notebook
# MAGIC ### I'm using %run to execute multiple Notebooks because they are starting streams and I need the Notebooks to return control back to this one.
# MAGIC ### The streams will continue to run until they are stopped.
# MAGIC ### These could also be run as Databricks Jobs.

# COMMAND ----------

# MAGIC %run ./ingestion/00_etl_streaming_bronze $triggerOnce=false

# COMMAND ----------

# MAGIC %run ./ingestion/00_etl_streaming_silver $triggerOnce=false

# COMMAND ----------

# MAGIC %run ./features/20_features $triggerOnce=false

# COMMAND ----------

# MAGIC %run ./inference/50_inference $triggerOnce=false
