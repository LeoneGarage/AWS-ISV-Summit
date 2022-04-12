# Databricks notebook source
# MAGIC %run ./udfs

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
safe_user_name = user.split("@")[0].replace(".", "_").replace("-", "_").replace("+", "_")
database_name = f'{safe_user_name}_db'
model_name = f'{safe_user_name}_InsuranceFraud'

# COMMAND ----------

location = f'/mnt/fraud/insurance/incoming'
targetLocation = f'/mnt/{database_name}/insurance/features/etl'
checkpointLocation = f'/mnt/{database_name}/insurance/features/etl/cp'
schemaLocation = f'/mnt/{database_name}/insurance/incoming_schema/etl_schema'

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

print(f'bronze_table_name = {database_name}.insurance_claims_bronze')
print(f'silver_table_name = {database_name}.insurance_claims_silver')
print(f'gold_table_name = {database_name}.insurance_claims_gold')
print(f'features_table_name = {database_name}.insurance_claims_features')
