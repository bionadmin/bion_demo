# Databricks notebook source
# MAGIC %md
# MAGIC ###Optimize Gold Tables

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

# MAGIC %md
# MAGIC ####Process table DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.`DimCustomer`')

# COMMAND ----------

#Done.
    

