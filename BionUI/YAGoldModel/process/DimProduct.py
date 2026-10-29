# Databricks notebook source
# MAGIC %md
# MAGIC <!-- your logo here! -->
# MAGIC <img src="https://github.com/bionadmin/distrib/raw/main/core.png"/>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Build DimProduct Gold Table

# COMMAND ----------

# MAGIC %md
# MAGIC #####Usage:
# MAGIC Note: You have "Include Table Create" disabled,
# MAGIC So be sure to run the schema update before running this notebook.
# MAGIC
# MAGIC This file may have been auto generated.<br>
# MAGIC This template expects source data to exist in one or more Silver tables which<br>
# MAGIC the query in step "Get Source Data" will reference. The Silver tables should<br>
# MAGIC also be Delta Lake tables.<br><br>
# MAGIC Modify the query in step "Get Source Data" using the provided template as guidance.<br>
# MAGIC Once written, queries may be stored in a BION model or in a file directory with <br>
# MAGIC the name DimProduct.sql so that subsequent builds do not overwrite the query<br>
# MAGIC with the template.<br><br>
# MAGIC The rest of the notebook is auto generated and may remain unchanged.<br>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Custom Beginning Cell

# COMMAND ----------

#Add custom python code into your Bion Table setting "Custom Beginning Cell" and it will appear here,
#Place %sql on the very first line to add custom sql code instead.
#Use <sourcedatabase>, <sourcetable>, <destinationdatabase>, <destinationtable> or <sourceschema> tokens to replace your code with the corresponding values
print("Custom Cell");

# COMMAND ----------

# MAGIC %md
# MAGIC ####Debug and Dev Options

# COMMAND ----------

#Be sure to set debugMode to False when not debugging -- especially if you recieve "The output of the notebook is too large" errors.
#Definitely set to false in production.
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
# MAGIC ####Write Source Catalog Name to Variable

# COMMAND ----------

# if the scope key named datalakesilvercatalog is found in the secret scope,
# use the secret as the catalog name, otherwise use the default Silver

source_catalog_name="Silver"
source_catalog_name_override=None
try:
    source_catalog_name_override=dbutils.secrets.get(scope="BionKeyvault",key="datalakesilvercatalog")
    source_catalog_name = source_catalog_name_override or source_catalog_name
except:
    if debug_mode:
        print('No catalog override found in keyvault. Using default value.')

spark.sql(f"DECLARE OR REPLACE VARIABLE var_source_catalog_name STRING DEFAULT '`{source_catalog_name}`'");

#NOTE: Use the variable var_source_catalog_name in your sql queries if your catalog may vary from dev to test to prod


# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Session and Table-specific Variables

# COMMAND ----------

import uuid
from datetime import datetime
#Session Variables

destination_database_name="YAGoldModel"
destination_table_name="DimProduct"
# Capture the RunID So that table logs can be grouped by run
dbutils.widgets.removeAll()
dbutils.widgets.text("run_grouping_id", "" , "Run Grouping ID")
dbutils.widgets.text("run_dttm", "" , "Run Start DTTM")

run_grouping_id = dbutils.widgets.get("run_grouping_id") 
run_dttm = dbutils.widgets.get("run_dttm") 
if not run_grouping_id:
    if debug_mode:
        print("Grouping ID from parent not found. Creating new Grouping ID")
    run_grouping_id = str(uuid.uuid4())
else:
    if debug_mode:
        print("Parent Grouping ID found.")

if not run_dttm:
    if debug_mode:
        print("Start DTTM from parent not found. Creating new Start DTTM")
    run_dttm = str(datetime.now())
else:
    if debug_mode:
        print("Parent Grouping ID found.")

if debug_mode:
    print("Grouping ID:"+run_grouping_id)    
spark.sql(f"DECLARE OR REPLACE VARIABLE var_run_grouping_id STRING DEFAULT '{run_grouping_id}'");
spark.sql(f"DECLARE OR REPLACE VARIABLE var_run_dttm STRING DEFAULT '{run_dttm}'");



# COMMAND ----------

# MAGIC %md 
# MAGIC ####Misc. Spark Options
# MAGIC If generating code, edit these in the template settings for consistency<br>
# MAGIC across all notebooks.

# COMMAND ----------

import pyspark
spark.sql(f"USE `{destination_catalog_name}`.{destination_database_name}")
spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
try:
    spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
except pyspark.errors.exceptions.connect.AnalysisException as e:
    if e.getErrorClass()=="CONFIG_NOT_AVAILABLE":
      if debug_mode:
        print("Auto optimize not available (this happens if you are using serverless).")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET ANSI_MODE = TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC ####Log table processing start

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_RuntimeTableLog')
# MAGIC (
# MAGIC   GroupingID,
# MAGIC   StartDateTime,
# MAGIC   DatabaseName,
# MAGIC   TableName,
# MAGIC   TableStartTime,
# MAGIC   TableEndTime,
# MAGIC   TableDurationSeconds,
# MAGIC   RowsAffected,
# MAGIC   RowsInserted,
# MAGIC   RowsUpdated,
# MAGIC   RowsDeleted,
# MAGIC   RowsBefore,
# MAGIC   RowsAfter,
# MAGIC   Audit_Status,
# MAGIC   Audit_CreatedDateTime,
# MAGIC   Audit_ModifiedDateTime
# MAGIC )
# MAGIC SELECT 
# MAGIC   SRC.GroupingID,
# MAGIC   SRC.StartDateTime,
# MAGIC   SRC.DatabaseName,
# MAGIC   SRC.TableName,
# MAGIC   SRC.TableStartTime,
# MAGIC   SRC.TableStartTime,
# MAGIC   0,
# MAGIC   0,
# MAGIC   0,
# MAGIC   0,
# MAGIC   0,
# MAGIC   SRC.RowsBefore,
# MAGIC   0,
# MAGIC   'Processing',
# MAGIC   SRC.TableStartTime,
# MAGIC   SRC.TableStartTime
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     var_run_grouping_id AS GroupingID,
# MAGIC     var_run_dttm AS StartDateTime,
# MAGIC     var_destination_catalog_name||'.YAGoldModel' AS DatabaseName,
# MAGIC     'DimProduct' AS TableName,
# MAGIC     current_timestamp() AS TableStartTime,
# MAGIC     (select count(*) from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct')) AS RowsBefore
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT GroupingID from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_RuntimeTableLog') WHERE GroupingID = var_run_grouping_id 
# MAGIC         AND DatabaseName = var_destination_catalog_name||'.YAGoldModel' 
# MAGIC         AND TableName = 'DimProduct' 
# MAGIC     )
# MAGIC ) SRC;
# MAGIC
# MAGIC --In cases where we are restarting a specific GroupingID run...
# MAGIC UPDATE IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_RuntimeTableLog')
# MAGIC   SET Audit_Status = 'Processing'
# MAGIC WHERE GroupingID = var_run_grouping_id
# MAGIC   AND DatabaseName = var_destination_catalog_name||'.YAGoldModel'
# MAGIC   AND TableName = 'DimProduct' 
# MAGIC   AND Audit_Status <> 'Processing'
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md
# MAGIC ####Custom Pre Query Cell

# COMMAND ----------

#Add custom python code into your Bion Table setting "Custom Pre Query Cell" and it will appear here,
#Place %sql on the very first line to add custom sql code instead.
#Use <sourcedatabase>, <sourcetable>, <destinationdatabase>, <destinationtable> or <sourceschema> tokens to replace your code with the corresponding values
print("Custom Cell");

# COMMAND ----------

# MAGIC %md
# MAGIC ####Get Source Data
# MAGIC <BR>Modify query as needed.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW src_data_for_DimProduct AS
# MAGIC WITH SessionVariables AS (
# MAGIC     --There seems to be a bug where variables are not recognized in the identifier within a vew if the variable is not referenced
# MAGIC     --first elsewhere. This is the workaround so we can include these variables in the query if desired...
# MAGIC     --no need to actually use the values in this cte. The variables will work once "mentioned".
# MAGIC     SELECT var_source_catalog_name,var_destination_catalog_name 
# MAGIC )
# MAGIC select ProductID, Name as ProductName, ProductNumber as ProductNumber, Color as ProductColor from silverdemo.mynewsilverschema.product
# MAGIC       /*
# MAGIC       --TEMPLATE BELOW:
# MAGIC SELECT 
# MAGIC           --*******************************************************
# MAGIC           --BEGIN CUSTOM BLOCK: Edit the following query as needed.
# MAGIC           --Be sure to keep the column aliases which should match the
# MAGIC           --destination column name(s)
# MAGIC
# MAGIC           --If generating this notebook, this query can be stored in 
# MAGIC           --the data model in a table setting named "Gold Table Query".
# MAGIC           --This will allow you to persist this query while
# MAGIC           --generating these notebooks. Note that if columns are added or removed from
# MAGIC           --the main table, the queries must be modified.
# MAGIC           --IF USING CTE's. MAKE SURE THERE ARE NO COMMENTS OR OTHER LINES OF CODE
# MAGIC           --before the intitial "WITH" statement (except for commented lines that begin with --) 
# MAGIC           --as this can cause issues with parsing.
# MAGIC           --*******************************************************
# MAGIC           t1.c0 AS ProductID,
# MAGIC           t1.c1 AS ProductName,
# MAGIC           t1.c2 AS ProductNumber,
# MAGIC           t1.c3 AS ProductColor,
# MAGIC           current_timestamp() AS RIGHTNOW--leave this.
# MAGIC       FROM Silver_sales.Some_Silver_Table t1
# MAGIC       --LEFT OUTER JOIN Silver_sales.Some_OTHER_Silver_Table T2
# MAGIC       --ON T1.SomeValue = T2.SomeValue
# MAGIC       --WHERE SomethingIsPerhaps > Something Or SomethingElse
# MAGIC       --*******************************************************
# MAGIC       --END CUSTOM BLOCK
# MAGIC       --*******************************************************
# MAGIC
# MAGIC       */

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####Review the Source Data
# MAGIC <BR>Modify query as desired to review the source data.

# COMMAND ----------

if debug_mode:
    display(spark.sql('select * from src_data_for_DimProduct LIMIT 1000'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Upsert Data

# COMMAND ----------

# MAGIC %sql
# MAGIC --Upsert data into table YAGoldModel.DimProduct
# MAGIC
# MAGIC
# MAGIC MERGE INTO IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct') DST
# MAGIC USING (
# MAGIC  SELECT
# MAGIC   *
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC           unhex(ProductKey__HEXSTRING) AS ProductKey,
# MAGIC           cast(conv(left(ProductKey__HEXSTRING,15),16,10) as bigint) AS ProductKeyInt,
# MAGIC           ProductID,
# MAGIC           ProductName,
# MAGIC           ProductNumber,
# MAGIC           ProductColor,
# MAGIC   
# MAGIC           ROW_NUMBER() OVER (PARTITION BY ProductKey__HEXSTRING ORDER BY ProductKey__HEXSTRING) DUPLICATES_RN,--nothing good to order by.
# MAGIC   
# MAGIC           RIGHTNOW  
# MAGIC
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC           SHA2(
# MAGIC              CAST(IFNULL(ProductID,'NULL') AS STRING),256) AS ProductKey__HEXSTRING,
# MAGIC           CAST(ProductID AS BIGINT) AS ProductID,
# MAGIC           CAST(COALESCE(ProductName,'') AS STRING) AS ProductName,
# MAGIC           CAST(COALESCE(ProductNumber,'') AS STRING) AS ProductNumber,
# MAGIC           CAST(COALESCE(ProductColor,'') AS STRING) AS ProductColor,
# MAGIC     
# MAGIC           RIGHTNOW          
# MAGIC
# MAGIC     FROM src_data_for_DimProduct
# MAGIC     ) SRC1
# MAGIC
# MAGIC   ) SRC2 WHERE DUPLICATES_RN = 1
# MAGIC ) SRC
# MAGIC ON SRC.ProductKey = DST.ProductKey
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   DST.ProductName = SRC.ProductName,
# MAGIC   DST.ProductNumber = SRC.ProductNumber,
# MAGIC   DST.ProductColor = SRC.ProductColor,
# MAGIC   DST.Audit_IsDeleted=false,
# MAGIC  
# MAGIC   DST.Audit_ModifiedDateTime = SRC.RIGHTNOW
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     ProductKey,
# MAGIC     ProductKeyInt,
# MAGIC     ProductID,
# MAGIC     ProductName,
# MAGIC     ProductNumber,
# MAGIC     ProductColor,
# MAGIC     Audit_IsDeleted,
# MAGIC  
# MAGIC     Audit_CreatedDateTime,
# MAGIC     Audit_ModifiedDateTime
# MAGIC     
# MAGIC  )
# MAGIC  VALUES(
# MAGIC    SRC.ProductKey,
# MAGIC    SRC.ProductKeyInt,    
# MAGIC    SRC.ProductID,
# MAGIC    SRC.ProductName,
# MAGIC    SRC.ProductNumber,
# MAGIC    SRC.ProductColor,
# MAGIC    false,
# MAGIC  
# MAGIC    SRC.RIGHTNOW,
# MAGIC    SRC.RIGHTNOW
# MAGIC  )
# MAGIC

# COMMAND ----------

rowsupserteddf=None
try:
    rowsupserteddf = _sqldf
except:
    #catch the not defined error
    print("Warning: _sqldf not defined")
    rowsupserteddf = spark.sql("select CAST(-1 as BIGINT) as num_affected_rows, CAST(-1 as BIGINT) as num_updated_rows, CAST(-1 as BIGINT) as num_deleted_rows, CAST(-1 as BIGINT) as num_inserted_rows") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Process Deletes 

# COMMAND ----------

# MAGIC %sql
# MAGIC --Delete data from the YAGoldModel.DimProduct table
# MAGIC
# MAGIC
# MAGIC MERGE INTO IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct') DST
# MAGIC USING (
# MAGIC  SELECT 
# MAGIC     ProductID,--primary or alternate key
# MAGIC     current_timestamp() AS RIGHTNOW--leave this.
# MAGIC  FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct') InTable 
# MAGIC  LEFT ANTI JOIN src_data_for_DimProduct InSource
# MAGIC ON 1=1
# MAGIC  AND (InTable.ProductID=InSource.ProductID)
# MAGIC ) SRC
# MAGIC ON
# MAGIC 1=1
# MAGIC  AND (DST.ProductID=SRC.ProductID)
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC  Audit_IsDeleted = true,
# MAGIC  Audit_ModifiedDateTime =  current_timestamp()
# MAGIC

# COMMAND ----------

rowsdeleteddf=None
try:
    rowsdeleteddf = _sqldf
except:
    print("Warning: _sqldf not defined")
    rowsdeleteddf = spark.sql("select CAST(-1 as BIGINT) as num_affected_rows, CAST(-1 as BIGINT) as num_updated_rows, CAST(-1 as BIGINT) as num_deleted_rows, CAST(-1 as BIGINT) as num_inserted_rows")





# COMMAND ----------

# MAGIC %md
# MAGIC ####Log table completion

# COMMAND ----------

rowsupserteddf.createOrReplaceTempView("tmp_YAGoldModel_DimProduct_upserted");
if ("rowsdeleteddf" in locals()):
    rowsdeleteddf.createOrReplaceTempView("tmp_YAGoldModel_DimProduct_deleted")
else:
    #cannot make another view off of rowsupserteddf as this will fail when attempting to join two views from the same dataframe. Thus:
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tmp_YAGoldModel_DimProduct_deleted AS (SELECT * FROM tmp_YAGoldModel_DimProduct_upserted)")
    

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC UPDATE IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_RuntimeTableLog') 
# MAGIC SET
# MAGIC   TableEndTime = current_timestamp(),
# MAGIC   TableDurationSeconds = datediff(SECOND,TableStartTime, current_timestamp()),
# MAGIC   RowsAffected = COALESCE(RowsAffected,0)+(
# MAGIC     select u.num_affected_rows+d.num_deleted_rows 
# MAGIC     FROM tmp_YAGoldModel_DimProduct_upserted u
# MAGIC     CROSS JOIN tmp_YAGoldModel_DimProduct_deleted d
# MAGIC   ),
# MAGIC   RowsInserted = COALESCE(RowsInserted,0)+(select num_inserted_rows from tmp_YAGoldModel_DimProduct_upserted),
# MAGIC   RowsUpdated = COALESCE(RowsUpdated,0)+(select num_updated_rows from tmp_YAGoldModel_DimProduct_upserted),
# MAGIC   RowsDeleted = COALESCE(RowsDeleted,0)+(select num_deleted_rows from tmp_YAGoldModel_DimProduct_deleted),
# MAGIC   RowsAfter = (select count(*) from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct')),
# MAGIC   Audit_Status = 'Completed',
# MAGIC   Audit_ModifiedDateTime = current_timestamp()
# MAGIC WHERE
# MAGIC   GroupingID = var_run_grouping_id
# MAGIC   AND DatabaseName = var_destination_catalog_name||'.YAGoldModel'
# MAGIC   AND TableName = 'DimProduct'
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC %md
# MAGIC ####Review the Upserted Data
# MAGIC <BR>Modify query as desired to review the upserted data.

# COMMAND ----------

if debug_mode:
    display(spark.sql("select * from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.DimProduct') ORDER BY Audit_ModifiedDateTime DESC LIMIT 1000"))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####Custom End Cell

# COMMAND ----------

#Add custom python code into your Bion Table setting "Custom End Cell" and it will appear here,
#Place %sql on the very first line to add custom sql code instead.
#Use <sourcedatabase>, <sourcetable>, <destinationdatabase>, <destinationtable> or <sourceschema> tokens to replace your code with the corresponding values
print("Custom Cell");

# COMMAND ----------

# MAGIC %md
# MAGIC ####End of Notebook
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
