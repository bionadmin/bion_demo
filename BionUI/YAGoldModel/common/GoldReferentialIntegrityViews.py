# Databricks notebook source
# MAGIC %md
# MAGIC <!-- your logo here! -->
# MAGIC <img src="https://github.com/bionadmin/distrib/raw/main/core.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #Referential Integrity Statistics
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC The audit_referentialintegrity view tracks the referential integrity between <BR>
# MAGIC related tables in the model by counting the number of nulls as well as the hash <BR>
# MAGIC values of nulls, empty strings, and zeros stored in the foreign keys of the BaseTable.  <BR>
# MAGIC It also counts the number of missing and valid foreign records in the ForeignTable. <BR>
# MAGIC The view provides insights into the quality of data relationships between tables <BR>
# MAGIC and can be used to identify and resolve data inconsistencies.

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

#No relationships in model.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####End of Notebook
# MAGIC
# MAGIC
