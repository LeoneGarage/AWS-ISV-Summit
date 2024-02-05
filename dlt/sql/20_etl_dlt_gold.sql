-- Databricks notebook source
CREATE STREAMING LIVE TABLE insurance_claims_gold_claims_amounts_city AS
  SELECT incident_city, SUM(total_claim_amount) as claim_amount
  FROM stream(LIVE.insurance_claims_silver)
  GROUP BY incident_city

-- COMMAND ----------

CREATE STREAMING LIVE TABLE insurance_claims_gold_hobbies AS
  SELECT insured_hobbies, SUM(total_claim_amount) as claim_amount
  FROM stream(LIVE.insurance_claims_silver)
  GROUP BY insured_hobbies

-- COMMAND ----------

CREATE STREAMING LIVE TABLE insurance_claims_gold_auto_type AS
  SELECT auto_make, auto_model, SUM(total_claim_amount) as claim_amount
  FROM stream(LIVE.insurance_claims_silver)
  GROUP BY auto_make, auto_model

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE insurance_claims_gold_claims_amounts

-- COMMAND ----------

APPLY CHANGES INTO live.insurance_claims_gold_claims_amounts
FROM stream(live.insurance_claims_gold_claims_amounts_city)
  KEYS (incident_city)
  SEQUENCE BY incident_city --primary key, auto-incrementing ID of any kind that can be used to identity order of events, or timestamp
-- COLUMNS * EXCEPT (operation, operation_date, _rescued_data)
