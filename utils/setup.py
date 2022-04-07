# Databricks notebook source
# MAGIC %run ./udfs

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
database_name = user.split("@")[0].replace(".", "_").replace("-", "_").replace("+", "_")
bronze_database_name = f'{database_name}_bronze'
silver_database_name = f'{database_name}_silver'
gold_database_name = f'{database_name}_gold'
features_database_name = f'{database_name}_features'
model_name = f'{database_name}_InsuranceFraud'

# COMMAND ----------

location = f'/mnt/fraud/insurance/incoming'
targetLocation = f'/mnt/{database_name}/insurance/features/etl'
checkpointLocation = f'/mnt/{database_name}/insurance/features/etl/cp'
schemaLocation = f'/mnt/{database_name}/insurance/incoming_schema/etl_schema'

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

print(f'bronze_database_name = {bronze_database_name}')
print(f'silver_database_name = {silver_database_name}')
print(f'gold_database_name = {gold_database_name}')
print(f'features_database_name = {features_database_name}')
