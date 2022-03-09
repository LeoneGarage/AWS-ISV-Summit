# Databricks notebook source
import inspect
import os

def registerFunction(key, func):
  lines = inspect.getsource(func)
  os.environ[key] = lines

# COMMAND ----------

import datetime
import pandas as pd
from pyspark.sql.functions import pandas_udf, udf, when, dayofweek, col

def parseDate(date, pattern):
  dd = datetime.datetime.strptime(date, pattern)
  return dd.replace(year = (dd.year-2000) + (2000 if (dd.year-2000) < 30 else 1900))

@pandas_udf("date string, pattern string")
def parseDateUdf(date: pd.Series, pattern: pd.Series) -> pd.Series:
  return date.apply(parseDate)

# COMMAND ----------

spark.udf.register(name='custom_to_date', f=parseDate, returnType='date')

# COMMAND ----------

def columnConvert(col):
  if col == 'policy_bind_date' or col == 'incident_date':
    return f"custom_to_date({col}, '%d/%m/%y')"
  return col

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
