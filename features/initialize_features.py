# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
import pyspark.sql.types
from pyspark.sql.types import _parse_datatype_string

# COMMAND ----------

def toDDL(self):
    """
    Returns a string containing the schema in DDL format.
    """
    from pyspark import SparkContext
    sc = SparkContext._active_spark_context
    dt = sc._jvm.__getattr__("org.apache.spark.sql.types.DataType$").__getattr__("MODULE$")
    json = self.json()
    return dt.fromJson(json).toDDL()
pyspark.sql.types.DataType.toDDL = toDDL
pyspark.sql.types.StructType.fromDDL = _parse_datatype_string

# COMMAND ----------

def featuresClearCheckpoints():
  dbutils.fs.rm(f'{checkpointLocation}/insurance_fraud_features', True)

# COMMAND ----------

def feature_table_exists(name):
  fs = FeatureStoreClient()
  try:
    fs.get_table(name)
  except:
    return False
  return True

# COMMAND ----------

def featuresRecreateTables():
  fs = FeatureStoreClient()
  tableSchema = 'policy_number INT,policy_state STRING,policy_csl STRING,policy_deductible STRING,policy_annual_premium DOUBLE,umbrella_limit INT,insured_zip INT,insured_sex STRING,insured_education_level STRING,insured_occupation STRING,insured_hobbies STRING,insured_relationship STRING,capital_gains INT,capital_loss INT,incident_type STRING,collision_type STRING,incident_severity STRING,authorities_contacted STRING,incident_state STRING,incident_city STRING,incident_location STRING,incident_hour_of_the_day INT,number_of_vehicles_involved INT,property_damage STRING,bodily_injuries INT,witnesses INT,police_report_available STRING,total_claim_amount INT,injury_claim INT,property_claim INT,vehicle_claim INT,auto_make STRING,auto_model STRING,auto_year INT,incident_weekend_flag INT NOT NULL,months_as_customer INT,age INT'
  spark.sql(f'''
CREATE DATABASE IF NOT EXISTS {features_database_name}
''')
  if not feature_table_exists(name=f'{features_database_name}.insurance_fraud_features'):
    spark.sql(f'DROP TABLE IF EXISTS {features_database_name}.insurance_fraud_features')
    feature_table = fs.create_table(
      name=f'{features_database_name}.insurance_fraud_features',
      primary_keys=['policy_number', 'injury_claim', 'property_claim', 'vehicle_claim'],
      schema=pyspark.sql.types.StructType.fromDDL(tableSchema),
      description=f'These features are derived from the {features_database_name}.insurance_fraud_features table in the lakehouse.'
    )

# COMMAND ----------

featuresClearCheckpoints()
featuresRecreateTables()
