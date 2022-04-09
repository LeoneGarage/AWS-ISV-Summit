# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/LeoneGarage/AWS-ISV-Summit/blob/master/images/TrainingFeatures.png?raw=true" />
# MAGIC <img src="https://github.com/LeoneGarage/AWS-ISV-Summit/blob/master/images/InferenceFeatures.png?raw=true" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import common variables & functions

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

dbutils.widgets.text("triggerOnce", "true")

# COMMAND ----------

from pyspark.sql.functions import *
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table
import databricks.koalas as ks

# COMMAND ----------

triggerOnce = dbutils.widgets.getArgument("triggerOnce")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### This is where we do feature engineering.
# MAGIC ### We are reading the Change Data Feed from Delta and doing some minor transformations on the fly before saving the Feature Table as Delta table.

# COMMAND ----------

def compute_features(data):
  df = data

  df = (
        df.withColumn('incident_weekend_flag', when(dayofweek('incident_date') == 1, 1).when(dayofweek('incident_date') == 7, 1). otherwise(0))
          .withColumn('months_as_customer', months_between('incident_date', 'policy_bind_date').cast('int'))
          .withColumn('age', (months_between('incident_date', 'date_of_birth')/12).cast('int'))
       )

  # Drop columns which are not useful for ML
  df = df.drop('date_of_birth').drop('policy_bind_date').drop('incident_date').drop('_rescued_data')

  # Drop missing values
  df = df.dropna()

  return df

# COMMAND ----------

def incremental_features():
  # This table will be recomputed incrementally by reading the silver table stream
  # when it is updated.
  
  df = spark.readStream.format("delta").option("readChangeFeed", "true").table(f"{database_name}.insurance_claims_silver")
  return compute_features(df).where("_change_type != 'update_preimage'").drop('_change_type', '_commit_version', '_commit_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### We use Feature Store library to write our features to Feature Store

# COMMAND ----------

fs = FeatureStoreClient()

features_df = incremental_features()

if triggerOnce=='true':
  fs.write_table(df=features_df,
                 name=f'{database_name}.insurance_claims_features',
                 checkpoint_location=f'{checkpointLocation}/insurance_claims_features',
                 trigger={'once': True },
                 mode='merge')
else:
  fs.write_table(df=features_df,
                 name=f'{database_name}.insurance_claims_features',
                 checkpoint_location=f'{checkpointLocation}/insurance_claims_features',
                 mode='merge')

# COMMAND ----------

#display(spark.sql(f'''SELECT * FROM {database_name}.{features_table_name}'''))
