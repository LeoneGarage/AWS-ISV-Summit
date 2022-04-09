# Databricks notebook source
# MAGIC %run ../utils/setup

# COMMAND ----------

def goldClearCheckpoints():
  dbutils.fs.rm(f'{checkpointLocation}/insurance_claims_gold', True)

# COMMAND ----------

def goldRecreateTables():
  spark.sql(f'DROP TABLE IF EXISTS {database_name}.insurance_claims_gold')
  spark.sql(f"""
CREATE DATABASE IF NOT EXISTS {database_name}
""")
  spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_name}.insurance_claims_gold(
date_of_birth DATE,
policy_number INT,
policy_bind_date DATE,
policy_state STRING,
policy_csl STRING,
policy_deductible STRING,
policy_annual_premium DOUBLE,
umbrella_limit INT,
insured_zip INT,
insured_sex STRING,
insured_education_level STRING,
insured_occupation STRING,
insured_hobbies STRING,
insured_relationship STRING,
capital_gains INT,
capital_loss INT,
incident_date DATE,
incident_type STRING,
collision_type STRING,
incident_severity STRING,
authorities_contacted STRING,
incident_state STRING,
incident_city STRING,
incident_location STRING,
incident_hour_of_the_day INT,
number_of_vehicles_involved INT,
property_damage STRING,
bodily_injuries INT,
witnesses INT,
police_report_available STRING,
total_claim_amount INT,
injury_claim INT,
property_claim INT,
vehicle_claim INT,
auto_make STRING,
auto_model STRING,
auto_year INT,
incident_weekend_flag INT,
prediction INT
)
""")

# COMMAND ----------

goldClearCheckpoints()
goldRecreateTables()
