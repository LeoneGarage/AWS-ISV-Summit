-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW insurance_claims_silver_view
(
  CONSTRAINT valid_policy_number EXPECT (policy_number is not null) ON VIOLATION DROP ROW,
  CONSTRAINT valid_policy_report EXPECT (police_report_available = 'YES' OR police_report_available = 'NO')
)
AS
  SELECT
    custom_to_date(date_of_birth, '%d/%m/%y') as date_of_birth,
    cast(policy_number as int) as policy_number,
    custom_to_date(policy_bind_date, '%d/%m/%y') as policy_bind_date,
    policy_state,
    policy_csl,
    policy_deductible,
    cast(policy_annual_premium as double) as policy_annual_premium,
    cast(umbrella_limit as int) as umbrella_limit,
    cast(insured_zip as int) as insured_zip,
    insured_sex,
    insured_education_level,
    insured_occupation,
    insured_hobbies,
    insured_relationship,
    cast(capital_gains as int) as capital_gains,
    cast(capital_loss as int) as capital_loss,
    custom_to_date(incident_date, '%d/%m/%y') as incident_date,
    incident_type,
    collision_type,
    incident_severity,
    authorities_contacted,
    incident_state,
    incident_city,
    incident_location,
    cast(incident_hour_of_the_day as int) as incident_hour_of_the_day,
    cast(number_of_vehicles_involved as int) as number_of_vehicles_involved,
    property_damage,
    cast(bodily_injuries as int) as bodily_injuries,
    cast(witnesses as int) as witnesses,
    police_report_available,
    cast(total_claim_amount as int) as total_claim_amount,
    cast(injury_claim as int) as injury_claim,
    cast(property_claim as int) as property_claim,
    cast(vehicle_claim as int) as vehicle_claim,
    auto_make,
    auto_model,
    cast(auto_year as int) as auto_year
FROM stream(LIVE.insurance_claims_bronze)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE insurance_claims_silver;

-- COMMAND ----------

APPLY CHANGES INTO LIVE.insurance_claims_silver
FROM stream(LIVE.insurance_claims_silver_view)
KEYS (policy_number, property_claim, injury_claim, vehicle_claim)
SEQUENCE BY policy_number
