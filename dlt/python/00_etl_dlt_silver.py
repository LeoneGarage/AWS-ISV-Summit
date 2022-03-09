# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(name=f'insurance_claims_silver')
@dlt.expect_or_drop('valid_incident_severity', "incident_severity != '' ")
def incremental_silver():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
  def columnConvert(col):
    if col == 'policy_bind_date' or col == 'incident_date':
      return f"from_2d_date({col}, 'dd/MM/yy')"
    return col

  insurance_claims_input = dlt.read_stream(f"insurance_claims_bronze")
  return insurance_claims_input.selectExpr([f"{columnConvert(c)} as {c}" for c in insurance_claims_input.columns])
