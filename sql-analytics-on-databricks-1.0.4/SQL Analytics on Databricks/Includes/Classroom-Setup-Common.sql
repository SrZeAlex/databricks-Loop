-- Databricks notebook source
-- 1. Cria o schema (caso ainda não exista)
CREATE SCHEMA IF NOT EXISTS databrickstheloop.ops;

-- 2. Cria a tabela meta
CREATE OR REPLACE TABLE databrickstheloop.ops.meta (
    owner STRING,
    object STRING,
    key STRING,
    value STRING
);


-- COMMAND ----------

INSERT INTO databrickstheloop.ops.meta (owner, object, key, value)
VALUES
    (current_user(), NULL, 'username', current_user()),
    (current_user(), NULL, 'catalog_name', 'databrickstheloop'),
    (current_user(), NULL, 'schema_name', split_part(replace(current_user(),'.','_'), '@', 1)),
    (current_user(), NULL, 'paths.working_dir', CONCAT('/Volumes/databrickstheloop/ops/', split_part(replace(current_user(),'.','_'), '@', 1))),
    (current_user(), NULL, 'warehouse_name', 'shared_warehouse'),
    ('account users', NULL, 'datasets.ca_sales', 'dbacademy_ca_sales.v01'),
    ('account users', NULL, 'datasets.aus_sales', 'dbacademy_aus_sales.v01'),
    ('account users', NULL, 'datasets.retail', 'dbacademy_retail.v01'),
    ('account users', NULL, 'paths.datasets.retail', '/Volumes/dbacademy_retail/v01'),
    (current_user(), NULL, 'pseudonym', split_part(replace(current_user(),'.','_'), '@', 1));

-- COMMAND ----------

SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
FROM databrickstheloop.ops.meta;

-- COMMAND ----------

-- CREATE DA VARIABLE USING SQL FOR USER INFORMATION FROM THE META TABLE

-- Create a temp view storing information from the obs table.
CREATE OR REPLACE TEMP VIEW user_info AS
SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
FROM databrickstheloop.ops.meta;

-- Create SQL dictionary var (map)
DECLARE OR REPLACE DA MAP<STRING,STRING>;

-- Set the temp view in the DA variable
SET VAR DA = (SELECT * FROM user_info);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(DA.schema_name);

DROP VIEW IF EXISTS user_info;

-- COMMAND ----------

-- Set the default catalog + schema
USE CATALOG databrickstheloop;
USE SCHEMA IDENTIFIER(DA.schema_name);

-- COMMAND ----------

-- Check to make sure all necessary tables are in the user's schema. Otherwise return an error message
DECLARE OR REPLACE VARIABLE user_table_count INT DEFAULT 7;

DECLARE OR REPLACE VARIABLE required_table_list ARRAY<STRING> 
  DEFAULT ARRAY('aus_customers', 'aus_opportunities', 'aus_orders',  
                'ca_customers', 'ca_opportunities', 'ca_orders','au_products_lookup');

SELECT
  CASE 
    WHEN COUNT(table_name) = user_table_count THEN 'Lab Check, all tables available for your lab!'  -- Adjust '3' to the number of tables you are checking
    ELSE '---ERROR: Tables in your lab are missing. Please rerun notebook 0 - REQUIRED - Course Setup to setup your environment.---'
  END AS table_status
FROM information_schema.tables
WHERE table_schema = DA.schema_name AND 
      table_name IN (SELECT EXPLODE(required_table_list));
