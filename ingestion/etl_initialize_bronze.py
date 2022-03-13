# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

def bronzeClearCheckpoints():
  dbutils.fs.rm(f'{checkpointLocation}/insurance_claims_bronze', True)
  dbutils.fs.rm(schemaLocation, True)

# COMMAND ----------

def bronzeRecreateTables():
  spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {bronze_database_name}
""")
  spark.sql(f'''
DROP TABLE IF EXISTS {bronze_database_name}.insurance_claims
  ''')
  spark.sql(f'''
CREATE TABLE IF NOT EXISTS {bronze_database_name}.insurance_claims (
  date_of_birth STRING,
  policy_number STRING,
  policy_bind_date STRING,
  policy_state STRING,
  policy_csl STRING,
  policy_deductible STRING,
  policy_annual_premium STRING,
  umbrella_limit STRING,
  insured_zip STRING,
  insured_sex STRING,
  insured_education_level STRING,
  insured_occupation STRING,
  insured_hobbies STRING,
  insured_relationship STRING,
  capital_gains STRING,
  capital_loss STRING,
  incident_date STRING,
  incident_type STRING,
  collision_type STRING,
  incident_severity STRING,
  authorities_contacted STRING,
  incident_state STRING,
  incident_city STRING,
  incident_location STRING,
  incident_hour_of_the_day STRING,
  number_of_vehicles_involved STRING,
  property_damage STRING,
  bodily_injuries STRING,
  witnesses STRING,
  police_report_available STRING,
  total_claim_amount STRING,
  injury_claim STRING,
  property_claim STRING,
  vehicle_claim STRING,
  auto_make STRING,
  auto_model STRING,
  auto_year STRING,
  _rescued_data STRING)
USING delta
''')

# COMMAND ----------

bronzeClearCheckpoints()
bronzeRecreateTables()
dbutils.fs.rm(f'{location}/gen', True)
