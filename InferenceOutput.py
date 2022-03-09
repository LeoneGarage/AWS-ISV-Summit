# Databricks notebook source
# MAGIC %run ./utils/setup

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The following cell waits for gold table to be available before displaying predictions

# COMMAND ----------

df = (spark.readStream.format('delta').table(f'{gold_database_name}.insurance_claims')
          .groupBy(col('insured_hobbies'))
          .agg(sum('prediction').alias('prediction'))
          .orderBy('insured_hobbies'))

display(df)
