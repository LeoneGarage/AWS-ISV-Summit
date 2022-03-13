# Databricks notebook source
insurance_claims_input = spark.read.format('csv').options(header='true').load("/mnt/fraud/insurance/files/insurance_claims.csv")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
import uuid

def generateShuffled(insurance_claims_input):
  col_1_df = insurance_claims_input.select('date of birth').withColumn('rand', rand()).orderBy('rand')
  col_2_df = insurance_claims_input.select('property damage').withColumn('rand', rand()).orderBy('rand')
  col_3_df = insurance_claims_input.select('incident severity').withColumn('rand', rand()).orderBy('rand')
  col_4_df = insurance_claims_input.select('insured hobbies').withColumn('rand', rand()).orderBy('rand')

  df = insurance_claims_input.drop('date of birth').drop('property damage').drop('incident severity').drop('insured hobbies')

  window = Window().orderBy(lit('A'))
  col_1_with_row_num = col_1_df.withColumn("row_num", row_number().over(window))
  col_2_with_row_num = col_2_df.withColumn("row_num", row_number().over(window))
  col_3_with_row_num = col_3_df.withColumn("row_num", row_number().over(window))
  col_4_with_row_num = col_4_df.withColumn("row_num", row_number().over(window))
  col_5_with_row_num = df.withColumn("row_num", row_number().over(window))

  a = col_1_with_row_num \
            .join(col_2_with_row_num, on=['row_num']) \
            .join(col_3_with_row_num, on=['row_num']) \
            .join(col_4_with_row_num, on=['row_num']) \
            .join(col_5_with_row_num, on=['row_num']).select(['date of birth', 'property damage', 'incident severity', 'insured hobbies'] + df.columns) \
            .withColumn('policy number', lpad((rand() * 1000000).cast('int'), 6, '9').cast('integer')) \
            .withColumn('property claim', lpad((rand() * 1000000).cast('int'), 6, '9').cast('integer')) \
            .withColumn('injury claim', lpad((rand() * 1000000).cast('int'), 6, '9').cast('integer')) \
            .withColumn('vehicle claim', lpad((rand() * 1000000).cast('int'), 6, '9').cast('integer')) \
            .select(insurance_claims_input.columns).drop('fraud reported')
  return a

# COMMAND ----------

def generateNewClaims():
  a = generateShuffled(insurance_claims_input)

  a.dropDuplicates(['policy number', 'property claim', 'injury claim', 'vehicle claim']).repartition(1).write.format('csv').options(header='true').mode('overwrite').save("/mnt/fraud/insurance/files/gen/insurance_claims_incoming.csv.1")
  files = [f for f in dbutils.fs.ls("/mnt/fraud/insurance/files/gen/insurance_claims_incoming.csv.1") if f.name.endswith('.csv')]
  for f in files:
    dbutils.fs.mv(f.path, f"/mnt/fraud/insurance/incoming/gen/insurance_claims_incoming-{str(uuid.uuid4())}.csv")
    dbutils.fs.rm("/mnt/fraud/insurance/files/gen/insurance_claims_incoming.csv.1", True)

# COMMAND ----------

import time

while(True):
  generateNewClaims()
  time.sleep(5)
