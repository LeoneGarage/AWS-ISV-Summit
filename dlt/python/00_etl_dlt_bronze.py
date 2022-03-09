# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(name=f'insurance_claims_bronze')
def incremental_bronze():
  # Since we read the raw table as a stream, this bronze table is also
  # updated incrementally.

  insurance_claims_input = ( spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", True)
      .option("cloudFiles.schemaLocation", spark.conf.get("fraud_pipeline.schema_path"))
      .options(header='true')
      .load(spark.conf.get("fraud_pipeline.raw_path")) )

  return insurance_claims_input.selectExpr([f"`{c}` as {c.replace(' ', '_').replace('-', '_').replace('?', '')}" for c in insurance_claims_input.columns])
