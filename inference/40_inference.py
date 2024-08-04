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
  max_version = 0
  for v in [int(m.version) for m in MlflowClient().search_model_versions(f"name='leone_catalog.default.leon_eller_insurancefraud'")]:
    if v > max_version:
      max_version = v
  mlflow_client = MlflowClient()
  return mlflow_client.get_registered_model(f"leone_catalog.default.{model_name}")


# COMMAND ----------

insuranceFeaturesDf = spark.readStream.format("delta").option("readChangeFeed", "true").table(f"{database_name}.insurance_claims_features") \
                                    .where("_change_type != 'update_preimage'").drop('_change_type', '_commit_version', '_commit_timestamp')

# COMMAND ----------

get_latest_model(model_name)

# COMMAND ----------

model_info = get_latest_model(model_name)
model_uri = f'models:/{model_name}@{list(model_info.aliases.keys())[0]}'

# COMMAND ----------

insuranceClaimsDf = spark.read.format('delta').table(f'{database_name}.insurance_claims_silver')

# COMMAND ----------

def scoreFeatures(batchDf, batchId):
  outDf = fs.score_batch(model_uri, batchDf)
  deltaTable = DeltaTable.forName(spark, f'{database_name}.insurance_claims_gold')
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
            .option('checkpointLocation', f'{checkpointLocation}/insurance_claims_gold')

if triggerOnce=='true':
  query = query.trigger(once=True)

query.start()

# COMMAND ----------

#display(spark.sql(f'''SELECT * FROM {database_name}.insurance_claims_gold'''))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from leone_catalog.leon_eller_db.insurance_claims_features

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM leone_catalog.leon_eller_db.insurance_claims_gold
