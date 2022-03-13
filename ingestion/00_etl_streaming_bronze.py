# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/LeoneGarage/AWS-ISV-Summit/blob/master/images/TrainingBronze.png?raw=true" />
# MAGIC <img src="https://github.com/LeoneGarage/AWS-ISV-Summit/blob/master/images/InferenceBronze.png?raw=true" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import common variables & functions

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Notebook widgets which are also parameters

# COMMAND ----------

dbutils.widgets.text("triggerOnce", "true")

# COMMAND ----------

triggerOnce = dbutils.widgets.getArgument("triggerOnce")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stream input CSV files

# COMMAND ----------

insurance_claims_input = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", False)
      .option("cloudFiles.schemaLocation", schemaLocation)
      .option("cloudFiles.maxBytesPerTrigger", 100)
      .options(header='true')
      .load(location))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Replace column names that have spaces with underscores and remove question marks. These are invalid characters for parquet columns
# MAGIC ### Save resulting data into Bronze table. At this stage we haven't performed any transformations or data type conversions

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
