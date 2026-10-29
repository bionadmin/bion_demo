# Databricks notebook source
# MAGIC %md
# MAGIC ####Update Schema

# COMMAND ----------

# MAGIC %run "./GoldUpdateSchema"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Run Gold Notebooks

# COMMAND ----------

# MAGIC %run "./Functions"

# COMMAND ----------

debug_mode = False # Include debug select and print statements

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write Catalog info to Variables

# COMMAND ----------

# if the scope key named datalakegoldcatalog is found in the secret scope,
# use the secret as the catalog name, otherwise use the default Gold

destination_catalog_name="Gold"
destination_catalog_name_override=None
try:
    destination_catalog_name_override=dbutils.secrets.get(scope="BionKeyvault",key="datalakegoldcatalog")
    destination_catalog_name = destination_catalog_name_override or destination_catalog_name
except:
    if debug_mode:
        print('No catalog override found in keyvault. Using default value.')

spark.sql(f"DECLARE OR REPLACE VARIABLE var_destination_catalog_name STRING DEFAULT '`{destination_catalog_name}`'");

# COMMAND ----------

import uuid
from datetime import datetime
#Do not re-run this cell if you want to keep the current grouping
run_grouping_id = str(uuid.uuid4())
run_dttm = str(datetime.now())

if debug_mode:
    print("Current Run GroupingID is "+run_grouping_id)
params = {"run_grouping_id":run_grouping_id, "run_dttm":run_dttm}


notebook_timeout = 2400
max_concurrent_notebooks = 4
retries = 0

notebooks = [
	NotebookData("../process/DimCustomer", notebook_timeout, params),
]   
    
res = parallelNotebooks(notebooks, max_concurrent_notebooks)
result = [i.result(timeout=notebook_timeout+120) for i in res] # This is a blocking call.






    

