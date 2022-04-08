# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient, FeatureLookup

# COMMAND ----------

def create_training_set(fs, target_col):
  tbl = fs.get_table(f'{database_name}.{features_table_name}')

  lookup_keys = ['policy_number', 'injury_claim', 'property_claim', 'vehicle_claim']
  feature_names = [c for c in tbl.features if c not in lookup_keys]
#  feature_lookups = FeatureLookup(table_name=f'{database_name}.{features_table_name}', feature_names=feature_names, lookup_key=lookup_keys)
  feature_lookups = [FeatureLookup(table_name=f'{database_name}.{features_table_name}', feature_names=c, lookup_key=lookup_keys) for c in feature_names]
  responseDf = spark.read.format('delta').table(f'{database_name}.insurance_claims_response')
  training_set = fs.create_training_set(
      responseDf,
      feature_lookups = feature_lookups,
      label = target_col
    )
  return training_set
