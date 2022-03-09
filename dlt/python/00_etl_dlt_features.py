# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# COMMAND ----------

def compute_features(data):
  df = data

#   df = df.withColumn('incident_month', month('incident_date'))
#   df = df.withColumn('incident_day_of_month', dayofmonth('incident_date'))
#   df = df.withColumn('incident_day_of_week', dayofweek('incident_date'))
  df = df.withColumn('incident_weekend_flag', when(dayofweek('incident_date') == 1, 1).when(dayofweek('incident_date') == 7, 1). otherwise(0))

  if "fraud_reported" in df.columns:
    df = df.withColumn("fraud_reported_str", col("fraud_reported")).drop("fraud_reported") \
      .selectExpr("*", "CASE WHEN fraud_reported_str='Y' THEN 1 ELSE 0 END as fraud_reported").drop("fraud_reported_str")

  df = df.drop('policy_bind_date').drop('incident_date').drop('_rescued_data')

  # Drop missing values
  df = df.dropna()

  return df

# COMMAND ----------

@dlt.table(name=f'insurance_claims_features')
def incremental_features():
  # This table will be recomputed incrementalle by reading the silver table stream
  # when it is updated.

  df = dlt.read_stream('insurance_claims_silver')
  return compute_features(df)
