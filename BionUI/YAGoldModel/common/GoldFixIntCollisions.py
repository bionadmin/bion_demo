# Databricks notebook source
# MAGIC %md
# MAGIC <!-- your logo here! -->
# MAGIC <img src="https://github.com/bionadmin/distrib/raw/main/core.png"/>

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
# MAGIC #Find and resolve any integer key collisions.
# MAGIC <br>
# MAGIC Bion Gold models use a binary 256 bit hash of the source system keys<br>
# MAGIC to create the surrogate key for the table. Collisions (unintended duplicates)<br>
# MAGIC in this space are exceedingly unlikely. You can expect to insert<br>
# MAGIC 200,000,000,000,000,000,000,000,000,000,000,000,000 rows before having<br>
# MAGIC your first collision. So this probability is ignored as it is virtually<br>
# MAGIC imposible.<br>
# MAGIC <br>
# MAGIC Additionally, however, a 64 bit<br>
# MAGIC integer key is calculated for use in PowerBI models and other tools<br>
# MAGIC that may not work with the binary key. This integer key is calculated <br>
# MAGIC from a sub-set of the bits of the larger 256 bit key. Collisions in this<br>
# MAGIC 64 bit space are much more likely. Specifically, collisions become quite feasible<br>
# MAGIC in tables with a billion rows or more. <br>
# MAGIC <br>
# MAGIC This notebook locates and repairs any duplicate integer keys that may occur in the model.<br>
# MAGIC It is only necessary to run this notebook if you have large tables with billions of rows<br>
# MAGIC and you are using the integer version of the keys in tools such as PowerBI<br>
# MAGIC <br>
# MAGIC An alternative is to use a string version of the binary key (this can be created in a view, for example<br>
# MAGIC by wrapping the column in the hex() function) in PowerBI and ignore the calculated int column altogether.<br>
# MAGIC This string key may not perform as well as the generated integer key however.<br>
# MAGIC <br>
# MAGIC At any rate, models with only a few million records per table or less will likely not encounter any collisions.<br>
# MAGIC <br>
# MAGIC Note: The odds of collisions do not increase linearly with the number of records,<br>
# MAGIC but instead increase in an exponential-like manner as the number of existing records increase until the odds reach 100%<br>
# MAGIC at which point you have saturated the possible combinations for the number of bits in use.<br>
# MAGIC <br>
# MAGIC If n is the number of records in a table, and b is the number of bits in the hash, then the probable number of collisions <br>
# MAGIC that will have occurred in the table can be estimated by 2^-(b+1)*(n-1)*n, or, alternatively, .5*(n*(n-1)-n)/2^b<br>
# MAGIC where ^ means "to the power of."<br>
# MAGIC This is of course just an estimate. The odds of collisions increase if inferior hash functions are used.<br>
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ####Debug options

# COMMAND ----------

debug_mode = False # Include debug select and print statements

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md
# MAGIC ####Process table DimCustomer.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW new_collisions_DimCustomer as (
# MAGIC   SELECT 
# MAGIC     CustomerKey, 
# MAGIC     CustomerKeyInt, 
# MAGIC     cast(conv(left(hex(CustomerKey),15),16,10) as bigint) original_key,
# MAGIC     cast(conv(left(hex(CustomerKey),15),16,10) as bigint) +(row_number() OVER (ORDER BY Audit_CreatedDateTime DESC, CustomerKey)-1) AS NewKey
# MAGIC   FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimCustomer')
# MAGIC   WHERE CustomerKeyInt IN (
# MAGIC     SELECT CustomerKeyInt FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimCustomer')
# MAGIC     GROUP BY CustomerKeyInt HAVING COUNT(*) > 1
# MAGIC   ) 
# MAGIC )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####Review hash collisions on integer keys for table DimCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM new_collisions_DimCustomer LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimCustomer') DST
# MAGIC USING (SELECT * FROM new_collisions_DimCustomer) AS SRC
# MAGIC ON SRC.CustomerKey = DST.CustomerKey
# MAGIC WHEN MATCHED THEN UPDATE SET CustomerKeyInt=src.NewKey

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --find all records that have ever been fixed. (Should never be many)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW all_fixed_records_DimCustomer as (
# MAGIC   SELECT CustomerKey, CustomerKeyInt
# MAGIC   FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimCustomer')
# MAGIC   WHERE CustomerKeyInt <> cast(conv(left(hex(CustomerKey),15),16,10) as bigint)
# MAGIC )

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC    
# MAGIC %md
# MAGIC ####End of Notebook
# MAGIC
