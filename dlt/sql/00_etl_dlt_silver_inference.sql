-- Databricks notebook source
CREATE LIVE TABLE insurance_claims_silver
  SELECT
  months_as_customer as months_as_customer,
  age as age,
  policy_number as policy_number,
  from_2d_date(policy_bind_date, 'dd/MM/yy') as policy_bind_date,
  policy_state as policy_state,
  policy_csl as policy_csl,
  policy_deductible as policy_deductible,
  policy_annual_premium as policy_annual_premium,
  umbrella_limit as umbrella_limit,
  insured_zip as insured_zip,
  insured_sex as insured_sex,
  insured_education_level as insured_education_level,
  insured_occupation as insured_occupation,
  insured_hobbies as insured_hobbies,
  insured_relationship as insured_relationship,
  capital_gains as capital_gains,
  capital_loss as capital_loss,
  from_2d_date(incident_date, 'dd/MM/yy') as incident_date,
  incident_type as incident_type,
  collision_type as collision_type,
  incident_severity as incident_severity,
  authorities_contacted as authorities_contacted,
  incident_state as incident_state,
  incident_city as incident_city,
  incident_location as incident_location,
  incident_hour_of_the_day as incident_hour_of_the_day,
  number_of_vehicles_involved as number_of_vehicles_involved,
  property_damage as property_damage,
  bodily_injuries as bodily_injuries,
  witnesses as witnesses,
  police_report_available as police_report_available,
  total_claim_amount as total_claim_amount,
  injury_claim as injury_claim,
  property_claim as property_claim,
  vehicle_claim as vehicle_claim,
  auto_make as auto_make,
  auto_model as auto_model,
  auto_year as auto_year
FROM LIVE.insurance_claims_bronze