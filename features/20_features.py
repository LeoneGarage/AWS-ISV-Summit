# Databricks notebook source
# MAGIC %md ## Data Processing
# MAGIC 
# MAGIC Next, we will clean up the data a little and prepare it for our machine learning model.
# MAGIC 
# MAGIC We will first remove the columns that we have identified earlier that have too many distinct categories and cannot be converted to numeric.

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### This is where we do feature engineering, though I'm only cleansing the data in this case.
# MAGIC ### I'm reading the Change Data Feed from Delta and doing some minor transformations on the fly before saving the Feature Table as Delta table
# MAGIC ### Note, I'm only deleting the checkpoint location for demo purposes, so the pipeline starts from beginning every time it's run. You wouldn't be doing this in production.
# MAGIC ### Also for more complex feature engineering requiring more complex aggregations, you would most likely transform into features as a periodic batch job and save to Delta and publish to low latency store as part of that.
# MAGIC ### You would still do inference from those features while streaming or via REST APIs, in real time. The features would then be lagging a bit behind inference.

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

def compute_features(data):
  df = data

#   df = df.withColumn('incident_month', month('incident_date'))
#   df = df.withColumn('incident_day_of_month', dayofmonth('incident_date'))
#   df = df.withColumn('incident_day_of_week', dayofweek('incident_date'))
  df = (
        df.withColumn('incident_weekend_flag', when(dayofweek('incident_date') == 1, 1).when(dayofweek('incident_date') == 7, 1). otherwise(0))
          .withColumn('months_as_customer', months_between('incident_date', 'policy_bind_date').cast('int'))
          .withColumn('age', (months_between('incident_date', 'date_of_birth')/12).cast('int'))
       )

  df = df.drop('date_of_birth').drop('policy_bind_date').drop('incident_date').drop('_rescued_data')

  # Drop missing values
  df = df.dropna()

  return df

# COMMAND ----------

def incremental_features():
  # This table will be recomputed incrementalle by reading the silver table stream
  # when it is updated.
  
  df = spark.readStream.format("delta").option("readChangeFeed", "true").table(f"{silver_database_name}.insurance_claims")
  return compute_features(df).where("_change_type != 'update_preimage'").drop('_change_type', '_commit_version', '_commit_timestamp')

# COMMAND ----------

# MAGIC %md Generate transformed dataset. This will be the dataset that we will use to create our machine learning models.

# COMMAND ----------

# MAGIC %md By selecting "label" and "fraud reported", we can infer that 0 corresponds to **No Fraud Reported** and 1 corresponds to **Fraud Reported**.

# COMMAND ----------

# MAGIC %md Next, split data into training and test sets.

# COMMAND ----------

fs = FeatureStoreClient()

features_df = incremental_features()

if triggerOnce=='true':
  fs.write_table(df=features_df,
                 name=f'{features_database_name}.insurance_fraud_features',
                 checkpoint_location=f'{checkpointLocation}/insurance_fraud_features',
                 trigger={'once': True },
                 mode='merge')
else:
  fs.write_table(df=features_df,
                 name=f'{features_database_name}.insurance_fraud_features',
                 checkpoint_location=f'{checkpointLocation}/insurance_fraud_features',
                 mode='merge')

# COMMAND ----------

#display(spark.sql(f'''SELECT * FROM {features_database_name}.insurance_fraud_features'''))
