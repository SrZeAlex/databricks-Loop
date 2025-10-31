# Databricks notebook source
# MAGIC %pip install jinja2

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import requests
import string
import json
import collections
import os
import string

# COMMAND ----------

def api_call(json_payload=None, request_type=None, api_endpoint=None):
  headers = {'Content-type': 'application/json', 'Accept':'*/*', 'Authorization': f'Bearer {TOKEN}'}
  if request_type == "POST":
    response = requests.post(f"{API_URL}{api_endpoint}", json=json_payload, headers=headers)
  elif request_type == "GET":
    response = requests.get(f"{API_URL}{api_endpoint}", json=json_payload, headers=headers)
  else:
    dbutils.notebook.exit(f"Invalid request type: {request_type}")
    return
  if response.status_code == 200: return response
  else: dbutils.notebook.exit(f"API call failed with status code {response.status_code}: {response.text}")

def get_dbr_versions(min_version=14.1):
  response = api_call(json_payload=None, request_type="GET", api_endpoint="/api/2.0/clusters/spark-versions")
  dbr_versions_list = json.loads(response.text)['versions']
  dbr_versions_dict = {}
  for dbr in dbr_versions_list:
    if not any(invalid in dbr['name'] for invalid in invalid_dbr_list):
      if float(dbr['name'].split(' ')[0]) >=  min_version:
        dbr_versions_dict[dbr['key']] = dbr['name']
  return collections.OrderedDict(sorted(dbr_versions_dict.items(), reverse=True))

# COMMAND ----------

@DBAcademyHelper.add_init
def display_config_values(self):
    config_values = [
        ('Catalog', self.catalog_name),
        ('TPCDI Schema', 'tpcdi_raw_data'),
        ('TPCDI Volume', 'tpcdi_volume'),
        ('SQL warehouse', self.warehouse_name)
   ]
    html = """<table style="width:100%">"""
    for name, value in config_values:
        html += f"""
        <tr>
            <td style="white-space:nowrap; width:1em">{name}:</td>
            <td><input type="text" value="{value}" style="width: 100%"></td></tr>"""
    html += "</table>"
    displayHTML(html)

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

# ENV-Specific API calls and variables
API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
if API_URL is None or TOKEN is None: 
  dbutils.notebook.exit("Unable to capture API/Token from dbutils. Please try again or open a ticket")
repo_src_path = os.getcwd()
workspace_src_path = repo_src_path
UC_enabled            = True
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
workflow_type     = 'DBSQL Warehouse Workflow'
job_name = f"{string.capwords(user_name).replace(' ','-')}-TPCDI"
pred_opt          = "DISABLE"
wh_target         = f"TPCDI"
incremental       = False
scale_factor      = 10
catalog           = DA.catalog_name
tpcdi_directory   = f'/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/'
lighthouse        = False
sku               = ['DBSQL', 'Warehouse', 'Workflow']
cloud_provider    = "AWS"
serverless        = 'NO'
invalid_dbr_list      = ['aarch64', 'ML', 'Snapshot', 'GPU', 'Photon', 'RC', 'Light', 'HLS', 'Beta', 'Latest']
dbrs                  = get_dbr_versions()
default_dbr_version   = list(dbrs.keys())[0]
default_worker_type = "m7gd.2xlarge"
default_driver_type = "m7gd.xlarge"
cust_mgmt_type      = "m7gd.16xlarge"

# COMMAND ----------

import os
import shutil

os_blob_out_path = f"{tpcdi_directory}sf={scale_factor}"

if os.path.exists(os_blob_out_path):
    print(f"Raw Data Directory exists. Skipping unpacking operation")
else:
    print(f"Raw Data Directory {os_blob_out_path} does not exist yet. Unpacking raw data for scale factor={scale_factor} into this directory")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")

    shutil.unpack_archive(
        "/Volumes/dbacademy/ops/assets/tpcdi_raw_data_sf10.zip",
        extract_dir=tpcdi_directory
    )

    print(f"Raw data unpack complete!")
