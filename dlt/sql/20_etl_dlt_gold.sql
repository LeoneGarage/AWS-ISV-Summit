-- Databricks notebook source
CREATE STREAMING LIVE TABLE insurance_claims_gold_claims_amounts AS
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
