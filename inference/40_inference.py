# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/LeoneGarage/AWS-ISV-Summit/blob/master/images/InferenceScoring.png?raw=true" />

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### In this case inference is done as part of the stream, using Feature Store score_batch() API. So the data coming in via a stream in this case could only be a policy_number. Since the features would already updated separately in the Feature Store, score_batch() API just joins the policy_number to the features in the Feature Store and does inference after that on the model associated with the features in 30_baseline_model Notebook when the model was fitted.
# MAGIC ### Note, I'm only deleting the checkpoint location and dropping the gold table for demo purposes so the pipeline starts from begining everytime it's run. You would not be doing this in production.

# COMMAND ----------

dbutils.widgets.text("triggerOnce", "true")

# COMMAND ----------

triggerOnce = dbutils.widgets.getArgument("triggerOnce")

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
from databricks.feature_store import FeatureStoreClient
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def get_latest_model(model_name):
  latest_version = 1
  mlflow_client = MlflowClient()
  return mlflow_client.get_latest_versions("InsuranceFraud", stages=['None'])[0]


# COMMAND ----------

insuranceFeaturesDf = spark.readStream.format("delta").option("readChangeFeed", "true").table(f"{features_database_name}.insurance_fraud_features") \
                                    .where("_change_type != 'update_preimage'").drop('_change_type', '_commit_version', '_commit_timestamp')

# COMMAND ----------

model_info = get_latest_model("InsuranceFraud")
model_uri = f'models:/InsuranceFraud/{model_info.version}'

# COMMAND ----------

insuranceClaimsDf = spark.read.format('delta').table(f'{silver_database_name}.insurance_claims')

# COMMAND ----------

def scoreFeatures(batchDf, batchId):
  outDf = fs.score_batch(model_uri, batchDf)
  deltaTable = DeltaTable.forName(spark, f'{gold_database_name}.insurance_claims')
  source = outDf
  deltaTable.alias("u").merge(
    source = source.alias("staged_updates"),
    condition = expr("u.policy_number = staged_updates.policy_number AND u.property_claim = staged_updates.property_claim AND u.injury_claim = staged_updates.injury_claim AND u.vehicle_claim = staged_updates.vehicle_claim")
  ).whenMatchedUpdateAll() \
   .whenNotMatchedInsertAll() \
   .execute()

# COMMAND ----------

fs = FeatureStoreClient()

query = insuranceFeaturesDf.join(insuranceClaimsDf, ['policy_number', 'injury_claim', 'property_claim', 'vehicle_claim']).select(insuranceClaimsDf['*'], insuranceFeaturesDf['incident_weekend_flag']) \
            .writeStream \
            .foreachBatch(scoreFeatures) \
            .option('checkpointLocation', f'{checkpointLocation}/gold_insurance_claims')

if triggerOnce=='true':
  query = query.trigger(once=True)

query.start()

# COMMAND ----------

#display(spark.sql(f'''SELECT * FROM {gold_database_name}.insurance_claims'''))
