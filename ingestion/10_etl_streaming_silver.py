# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://leone.z22.web.core.windows.net/images/TrainingSilver.png" />
# MAGIC <img src="https://leone.z22.web.core.windows.net/images/InferenceSilver.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import common variables & functions

# COMMAND ----------

# MAGIC %run ../utils/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Notebook widgets which are also parameters

# COMMAND ----------

dbutils.widgets.text("triggerOnce", "true")

# COMMAND ----------

triggerOnce = dbutils.widgets.getArgument("triggerOnce")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read incoming Bronze data as a stream. We know it's append only so we can just read as a standard append only stream and don't need to read as a CDF stream

# COMMAND ----------

bronzeInsuranceClaimsDf = spark.readStream.format("delta").table(f"{bronze_database_name}.insurance_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Since we are ingesting into Silver we will be doing Delta MERGE using foreachBatch() Structured Streaming feature.
# MAGIC ### This function is doing the actual microbatch batch MERGE and is called by Structured Streaming framework.
# MAGIC ### We do a merge by primary keys policy_number, property_claim, injury_claim, vehicle_claim. Matching rows are updated and non-matching rows are inserted.

# COMMAND ----------

def upsertToSilver(batchDf, batchId):
  deltaTable = DeltaTable.forName(spark, f'{silver_database_name}.insurance_claims')
  source = batchDf
  deltaTable.alias("u").merge(
    source = source.alias("staged_updates"),
    condition = expr("u.policy_number = staged_updates.policy_number AND u.property_claim = staged_updates.property_claim AND u.injury_claim = staged_updates.injury_claim AND u.vehicle_claim = staged_updates.vehicle_claim")
  ).whenMatchedUpdateAll() \
   .whenNotMatchedInsertAll() \
   .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### The writeStream() code below performs ingest into Silver.
# MAGIC ### We perform some simple transforms to convert field values to more appriate data types.
# MAGIC ### This is where you would do your tranform and MERGE before writing to Silver.

# COMMAND ----------

query = bronzeInsuranceClaimsDf \
  .selectExpr([
    "custom_to_date(date_of_birth, '%d/%m/%y') as date_of_birth",
    "cast(policy_number as int) as policy_number",
    "custom_to_date(policy_bind_date, '%d/%m/%y') as policy_bind_date",
    "policy_state",
    "policy_csl",
    "policy_deductible",
    "cast(policy_annual_premium as double) as policy_annual_premium",
    "cast(umbrella_limit as int) as umbrella_limit",
    "cast(insured_zip as int) as insured_zip",
    "insured_sex",
    "insured_education_level",
    "insured_occupation",
    "insured_hobbies",
    "insured_relationship",
    "cast(capital_gains as int) as capital_gains",
    "cast(capital_loss as int) as capital_loss",
    "custom_to_date(incident_date, '%d/%m/%y') as incident_date",
    "incident_type",
    "collision_type",
    "incident_severity",
    "authorities_contacted",
    "incident_state",
    "incident_city",
    "incident_location",
    "cast(incident_hour_of_the_day as int) as incident_hour_of_the_day",
    "cast(number_of_vehicles_involved as int) as number_of_vehicles_involved",
    "property_damage",
    "cast(bodily_injuries as int) as bodily_injuries",
    "cast(witnesses as int) as witnesses",
    "police_report_available",
    "cast(total_claim_amount as int) as total_claim_amount",
    "cast(injury_claim as int) as injury_claim",
    "cast(property_claim as int) as property_claim",
    "cast(vehicle_claim as int) as vehicle_claim",
    "auto_make",
    "auto_model",
    "cast(auto_year as int) as auto_year"
   ]) \
  .writeStream.format("delta") \
  .foreachBatch(upsertToSilver) \
  .option('checkpointLocation', f'{checkpointLocation}/insurance_claims_silver')

if triggerOnce=='true':
  query = query.trigger(once=True)

query.start()
#query.toTable(f'{silver_database_name}.insurance_claims')

# COMMAND ----------

#display(spark.sql(f'SELECT * FROM {silver_database_name}.insurance_claims'))
