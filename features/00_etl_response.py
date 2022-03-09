# Databricks notebook source
# MAGIC %run ../training/training_set

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient, FeatureLookup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### This is where we persist response file with policy_number and fraud_reported columns so we could join it later to the features for model fitting

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

responseLocation = '/mnt/fraud/insurance/fitting/insurance_claims.csv'

# COMMAND ----------

insurance_claims_response = spark.read.format('csv').options(header='true', inferSchema='true').load(responseLocation).select(col('policy number').alias('policy_number'), col('injury claim').alias('injury_claim'), col('property claim').alias('property_claim'), col('vehicle claim').alias('vehicle_claim'), col('fraud reported').alias('fraud_reported')).selectExpr("policy_number", "injury_claim", "property_claim", "vehicle_claim", "CASE WHEN fraud_reported='Y' THEN 1 ELSE 0 END as fraud_reported")

# COMMAND ----------

insurance_claims_response.createOrReplaceTempView("insurance_claims_response")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {features_database_name}.insurance_claims_response
USING DELTA AS
SELECT * FROM insurance_claims_response
""")

# COMMAND ----------

spark.sql(f'''
DROP TABLE IF EXISTS {features_database_name}.insurance_claims_training_set
''')

# COMMAND ----------

fs = FeatureStoreClient()
training_set = create_training_set(fs, 'fraud_reported')

trainingDf = training_set.load_df().dropna()
trainingDf.write.format('delta').mode('overwrite').saveAsTable(f'{features_database_name}.insurance_claims_training_set')

# COMMAND ----------

#display(spark.sql(f'SELECT * FROM {features_database_name}.insurance_claims_training_set'))
