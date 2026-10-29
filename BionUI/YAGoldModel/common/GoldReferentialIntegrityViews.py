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

# MAGIC %md
# MAGIC ####Analyze Referential Integrity of Foreign Keys

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.FactSales_Audit_ReferentialIntegrity') AS (
# MAGIC  WITH NullKeys AS (
# MAGIC   SELECT 
# MAGIC    unhex(SHA2('NULL',256)) AS NullHash,
# MAGIC    unhex(SHA2('',256)) AS EmptyStringHash,
# MAGIC    unhex(SHA2('0',256)) AS ZeroStringHash
# MAGIC   )
# MAGIC SELECT
# MAGIC     'FactSales' AS BaseTable,
# MAGIC     'CustomerKey' AS BaseTableForeignKey,
# MAGIC     'DimCustomer' AS ForeignTable,
# MAGIC     'CustomerKey' AS ForeignTableSurrogateKey,
# MAGIC     COALESCE(SUM(CASE WHEN f.`CustomerKey` IS NULL THEN 1 ELSE 0 END),0) AS NullForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN f.`CustomerKey` = k.NullHash THEN 1 ELSE 0 END),0) AS HashNullForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN f.`CustomerKey` = k.EmptyStringHash THEN 1 ELSE 0 END),0) AS HashEmptyStringForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN f.`CustomerKey` = k.ZeroStringHash THEN 1 ELSE 0 END),0) AS HashZeroForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN d.`CustomerKey` IS NULL AND f.`CustomerKey` IS NOT NULL THEN 1 ELSE 0 END),0) AS MissingForeignRecordCount,
# MAGIC     COALESCE(COUNT(DISTINCT CASE WHEN d.`CustomerKey` IS NULL AND f.`CustomerKey` IS NOT NULL THEN f.`CustomerKey` ELSE NULL END),0) AS DistinctMissingForeignRecordCount,
# MAGIC     COALESCE(SUM(CASE WHEN d.`CustomerKey` IS NOT NULL AND f.`CustomerKey` IS NOT NULL THEN 1 ELSE 0 END),0) AS ValidForeignRecordCount,
# MAGIC     COALESCE(COUNT(DISTINCT CASE WHEN d.`CustomerKey` IS NOT NULL AND f.`CustomerKey` IS NOT NULL THEN f.`CustomerKey` ELSE NULL END),0) AS DistinctValidForeignRecordCount,
# MAGIC     0 AS InferredForeignRecordCount,
# MAGIC     0 AS DistinctInferredForeignRecordCount,
# MAGIC     COALESCE(COUNT(f.`FactSalesKey`),0) AS TotalRecordCount
# MAGIC FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.FactSales') f
# MAGIC LEFT OUTER JOIN IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimCustomer') d
# MAGIC ON f.`CustomerKey` = d.`CustomerKey`
# MAGIC CROSS JOIN NullKeys k
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'FactSales' AS BaseTable,
# MAGIC     'ProductKey' AS BaseTableForeignKey,
# MAGIC     'DimProduct' AS ForeignTable,
# MAGIC     'ProductKey' AS ForeignTableSurrogateKey,
# MAGIC     COALESCE(SUM(CASE WHEN f.`ProductKey` IS NULL THEN 1 ELSE 0 END),0) AS NullForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN f.`ProductKey` = k.NullHash THEN 1 ELSE 0 END),0) AS HashNullForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN f.`ProductKey` = k.EmptyStringHash THEN 1 ELSE 0 END),0) AS HashEmptyStringForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN f.`ProductKey` = k.ZeroStringHash THEN 1 ELSE 0 END),0) AS HashZeroForeignKeyCount,
# MAGIC     COALESCE(SUM(CASE WHEN d.`ProductKey` IS NULL AND f.`ProductKey` IS NOT NULL THEN 1 ELSE 0 END),0) AS MissingForeignRecordCount,
# MAGIC     COALESCE(COUNT(DISTINCT CASE WHEN d.`ProductKey` IS NULL AND f.`ProductKey` IS NOT NULL THEN f.`ProductKey` ELSE NULL END),0) AS DistinctMissingForeignRecordCount,
# MAGIC     COALESCE(SUM(CASE WHEN d.`ProductKey` IS NOT NULL AND f.`ProductKey` IS NOT NULL THEN 1 ELSE 0 END),0) AS ValidForeignRecordCount,
# MAGIC     COALESCE(COUNT(DISTINCT CASE WHEN d.`ProductKey` IS NOT NULL AND f.`ProductKey` IS NOT NULL THEN f.`ProductKey` ELSE NULL END),0) AS DistinctValidForeignRecordCount,
# MAGIC     0 AS InferredForeignRecordCount,
# MAGIC     0 AS DistinctInferredForeignRecordCount,
# MAGIC     COALESCE(COUNT(f.`FactSalesKey`),0) AS TotalRecordCount
# MAGIC FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.FactSales') f
# MAGIC LEFT OUTER JOIN IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct') d
# MAGIC ON f.`ProductKey` = d.`ProductKey`
# MAGIC CROSS JOIN NullKeys k
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_ReferentialIntegrity') AS (
# MAGIC   SELECT * FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.FactSales_Audit_ReferentialIntegrity')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --Sample Queries...
# MAGIC --NOTE: Filtering on a Base table and additional columns may greatly speed the query on models with a large amount of records.
# MAGIC --SELECT * FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_ReferentialIntegrity') where BaseTable ='FactSales' and BaseTableForeignKey = 'CustomerKey'
# MAGIC --SELECT * FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_ReferentialIntegrity') where BaseTable ='FactSales' and BaseTableForeignKey = 'ProductKey'
# MAGIC --
# MAGIC --You can also copy the view into a table if you wish to have faster subsequent queries.
# MAGIC --(Useful for large models with a large amount of tables and foreign keys.)
# MAGIC --Example:
# MAGIC --CREATE OR REPLACE TABLE IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_ReferentialIntegrity_Materialized') AS
# MAGIC --SELECT * FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_ReferentialIntegrity')

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####End of Notebook
# MAGIC
# MAGIC
