# Databricks notebook source
# MAGIC %run ../../utils/setup

# COMMAND ----------

try:
  dbutils.widgets.remove('bronze_database_name')
  dbutils.widgets.remove('silver_database_name')
  dbutils.widgets.remove('gold_database_name')
  dbutils.widgets.remove('location')
except:
  pass

# COMMAND ----------

dbutils.widgets.dropdown('fitting', 'true', ['true', 'false'], 'Fitting Run')

# COMMAND ----------

if dbutils.widgets.getArgument('fitting') == 'true':
  fitSuffix = '_fit'
  location = '/mnt/fraud/insurance/dlt/fitting/tables'
else:
  fitSuffix = ''
  location = '/mnt/fraud/insurance/dlt/features/tables'

# COMMAND ----------

dbutils.widgets.text('bronze_database_name', bronze_database_name + fitSuffix, 'Bronze Database Name')
dbutils.widgets.text('silver_database_name', silver_database_name + fitSuffix, 'Silver Database Name')
dbutils.widgets.text('gold_database_name', gold_database_name + fitSuffix, 'Gold Database Name')
dbutils.widgets.text('location', location, 'Database File Location')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS $bronze_database_name cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS $bronze_database_name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS $silver_database_name cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS $silver_database_name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS $gold_database_name cascade

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS $gold_database_name

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS $bronze_database_name.insurance_claims

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE $bronze_database_name.insurance_claims
# MAGIC USING DELTA
# MAGIC LOCATION '$location/insurance_claims_bronze'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS $silver_database_name.insurance_claims

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE $silver_database_name.insurance_claims
# MAGIC USING DELTA
# MAGIC LOCATION '$location/insurance_claims_silver'

# COMMAND ----------

# spark.sql(f"""
# DROP TABLE IF EXISTS {gold_database_name}{fitSuffix}.insurance_claims
# """)

# COMMAND ----------

# spark.sql(f"""
# CREATE TABLE {gold_database_name}{fitSuffix}.insurance_claims
# USING DELTA
# LOCATION '{location}/insurance_claims_gold'
# """)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM $silver_database_name.insurance_claims
