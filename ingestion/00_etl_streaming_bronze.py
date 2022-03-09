# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### We stream the data in here and will then turn these into features in later Notebook
# MAGIC ### Note, I'm only deleting checkpoint files and tables here so that the pipeline starts from scratch for demonstration purposes. In production you wouldn't be doing that

# COMMAND ----------

dbutils.widgets.text("triggerOnce", "true")

# COMMAND ----------

triggerOnce = dbutils.widgets.getArgument("triggerOnce")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

insurance_claims_input = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", False)
      .option("cloudFiles.schemaLocation", schemaLocation)
      .option("cloudFiles.maxBytesPerTrigger", 100)
      .options(header='true')
      .load(location))

# COMMAND ----------

query = insurance_claims_input \
  .selectExpr([f"`{c}` as {c.replace(' ', '_').replace('-', '_').replace('?', '')}" for c in insurance_claims_input.columns]) \
  .writeStream.format("delta") \
  .option('checkpointLocation', f'{checkpointLocation}/insurance_claims_bronze')

if triggerOnce=='true':
  query = query.trigger(once=True)

query.toTable(f'{bronze_database_name}.insurance_claims')

# COMMAND ----------

#display(spark.sql(f'SELECT * FROM {bronze_database_name}.insurance_claims'))
