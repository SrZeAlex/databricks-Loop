-- Databricks notebook source
-- MAGIC %run ./Classroom-Setup-Common

-- COMMAND ----------

USE CATALOG databrickstheloop;
USE SCHEMA IDENTIFIER(DA.schema_name);

DROP TABLE IF EXISTS state_lookup;

CREATE TABLE state_lookup (
    state_abbreviation VARCHAR(2),
    state_name VARCHAR(100)
);

INSERT INTO state_lookup (state_abbreviation, state_name) VALUES
('AL', 'Alabama'),
('AK', 'Alaska'),
('AZ', 'Arizona'),
('AR', 'Arkansas'),
('CA', 'California'),
('CO', 'Colorado'),
('CT', 'Connecticut'),
('DE', 'Delaware'),
('FL', 'Florida'),
('GA', 'Georgia'),
('HI', 'Hawaii'),
('ID', 'Idaho'),
('IL', 'Illinois'),
('IN', 'Indiana'),
('IA', 'Iowa'),
('KS', 'Kansas'),
('KY', 'Kentucky'),
('LA', 'Louisiana'),
('ME', 'Maine'),
('MD', 'Maryland'),
('MA', 'Massachusetts'),
('MI', 'Michigan'),
('MN', 'Minnesota'),
('MS', 'Mississippi'),
('MO', 'Missouri'),
('MT', 'Montana'),
('NE', 'Nebraska'),
('NV', 'Nevada'),
('NH', 'New Hampshire'),
('NJ', 'New Jersey'),
('NM', 'New Mexico'),
('NY', 'New York'),
('NC', 'North Carolina'),
('ND', 'North Dakota'),
('OH', 'Ohio'),
('OK', 'Oklahoma'),
('OR', 'Oregon'),
('PA', 'Pennsylvania'),
('RI', 'Rhode Island'),
('SC', 'South Carolina'),
('SD', 'South Dakota'),
('TN', 'Tennessee'),
('TX', 'Texas'),
('UT', 'Utah'),
('VT', 'Vermont'),
('VA', 'Virginia'),
('WA', 'Washington'),
('WV', 'West Virginia'),
('WI', 'Wisconsin'),
('WY', 'Wyoming');

-- COMMAND ----------

-- Solution table

-- Drop source table if exists
DROP TABLE IF EXISTS customers_lab_bronze;

-- Create an empty table
CREATE TABLE customers_lab_bronze;

-- Copy JSON files from a volume into the table, merge the schema
COPY INTO customers_lab_bronze
FROM '/Volumes/dbacademy_retail/v01/retail-pipeline/customers/stream_json/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');

-- Create solution table
CREATE OR REPLACE TABLE customer_lab_silver_solution AS
SELECT
  cust.customer_id,
  cust.name,
  cust.address,
  cust.city,
  cust.email,
  cust.operation,
  cust.zip_code,
  cust.state,
  date_format(from_unixtime(timestamp), 'yyyy-MM-dd HH:mm:ss') AS sign_up_date,
  substring_index(cust.email, '@', -1) AS email_domain,
  state.state_name
FROM customers_lab_bronze cust
LEFT JOIN state_lookup state ON cust.state = state.state_abbreviation;

-- Drop source table if exists
DROP TABLE IF EXISTS customers_lab_bronze;

-- COMMAND ----------

-- CREATE OR REPLACE FUNCTION check_table_lab_3_4(user_table_name STRING)
-- RETURNS STRING
-- RETURN 

-- WITH 
--   -- Compare the columns names of the users table and the solution table
--   table1_schema AS (
--     SELECT column_name 
--     FROM INFORMATION_SCHEMA.COLUMNS
--     WHERE table_name = user_table_name AND
--           table_catalog = 'databrickstheloop'
--   ),
--   table2_schema AS (
--     SELECT column_name 
--     FROM INFORMATION_SCHEMA.COLUMNS
--     WHERE table_name = 'customer_lab_silver_solution' AND
--           table_catalog = 'databrickstheloop'
--   ),
--   schema_comparison AS (
--     SELECT
--       CASE
--         WHEN NOT EXISTS(
--             SELECT * FROM table1_schema
--             EXCEPT 
--             SELECT * FROM table2_schema
--         ) 
--         AND NOT EXISTS (
--             SELECT * FROM table2_schema
--             EXCEPT
--             SELECT * FROM table1_schema
--         )
--         THEN 'Schema matches the solution.'
--         ELSE 'Schemas not not matched'
--       END AS schema_comparison
--     ),

--   -- Compare the data in the tables
--   user_table AS (
--       SELECT * 
--       FROM IDENTIFIER(user_table_name)
--   ),
--   solution_table AS (
--       SELECT * 
--       FROM customer_lab_silver_solution
--   ),
--   data_comparison AS (
--     SELECT 
--         CASE 
--             WHEN NOT EXISTS (
--                 SELECT * FROM user_table
--                 EXCEPT
--                 SELECT * FROM solution_table
--             ) 
--             AND NOT EXISTS (
--                 SELECT * FROM solution_table
--                 EXCEPT
--                 SELECT * FROM user_table
--             ) 
--             THEN 'Your table matches the solution table customer_lab_silver_solution.'
--             ELSE 'Your customer_lab_silver table is incorrect and does not match the solution table customer_lab_silver_solution. Make sure your table is named correctly and you have followed all directions. You can view the solution table customer_lab_silver_solution to view the correct table.'
--         END AS data_comparison
--   ),
--   final_results AS (
--     SELECT *
--     FROM schema_comparison CARTESIAN JOIN data_comparison
--   )
-- -- Final comparing of the two
-- SELECT
--   CASE 
--     WHEN 
--       schema_comparison = 'Schema matches the solution.' AND 
--       data_comparison = 'Your table matches the solution table customer_lab_silver_solution.'
--     THEN 'Your table is correct!'
--     ELSE 'Your customer_lab_silver table is incorrect and does not match the customer_lab_silver_solution table. Ensure that your table is named correctly and that all instructions have been followed. You can refer to the customer_lab_silver_solution table to see the correct version.'
--   END AS Table_Checker
-- FROM final_results;
