# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### This Notebook runs the Fitting pipeline and builds a model which will be registered in Feature Store and Model Registry
# MAGIC ### I'm executing this pipeline using Notebook Workflows

# COMMAND ----------

dbutils.notebook.run("./00_etl_response", 0)

# COMMAND ----------

dbutils.notebook.run("./00_etl_streaming", 0, {"triggerOnce": "true" })

# COMMAND ----------

dbutils.notebook.run("./20_features", 0, {"triggerOnce": "true" })

# COMMAND ----------

dbutils.notebook.run("./30_baseline_model", 0)
