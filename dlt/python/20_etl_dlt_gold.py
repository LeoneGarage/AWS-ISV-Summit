# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# COMMAND ----------

@dlt.table(name=f'insurance_claims_gold_claims_amounts')
def insurance_claims_gold_claims_amounts():
  df = dlt.read_stream('insurance_claims_silver')
  return df.groupBy('incident_city').agg(sum('total_claim_amount').alias('claim_amount'))

# COMMAND ----------

@dlt.table(name=f'insurance_claims_gold_hobbies')
def insurance_claims_gold_hobbies():
  df = dlt.read_stream('insurance_claims_silver')
  return df.groupBy('insured_hobbies').agg(sum('total_claim_amount').alias('claim_amount'))

# COMMAND ----------

@dlt.table(name=f'insurance_claims_gold_auto_type')
def insurance_claims_gold_auto_type():
  df = dlt.read_stream('insurance_claims_silver')
  return df.groupBy('auto_make', 'auto_model').agg(sum('total_claim_amount').alias('claim_amount'))
