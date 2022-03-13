# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/LeoneGarage/AWS-ISV-Summit/blob/master/images/TrainingFeatures.png?raw=true" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### So far we have written a pipeline to ingest data and create features.
# MAGIC ### However, for ML Model training we need some response data i.e. examples of features where there was actual fraud reported.
# MAGIC ### What we do here is take that response file and join with original features to create a training set we cna use for ML training

# COMMAND ----------

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

# MAGIC %md
# MAGIC 
# MAGIC ### Load the response claims file with fraud_reported column, remove columns that are not needed and leave primary keys to join to features

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

# MAGIC %md
# MAGIC ### We are calling a function that will use Feature Store library to create a training set, which is a join between response file and features in Feature Store
# MAGIC ### We will use this training set to train our ML model.

# COMMAND ----------

fs = FeatureStoreClient()
training_set = create_training_set(fs, 'fraud_reported')

trainingDf = training_set.load_df().dropna()
trainingDf.write.format('delta').mode('overwrite').saveAsTable(f'{features_database_name}.insurance_claims_training_set')

# COMMAND ----------

#display(spark.sql(f'SELECT * FROM {features_database_name}.insurance_claims_training_set'))
