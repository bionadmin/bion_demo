# Databricks notebook source
# MAGIC %md
# MAGIC <!-- your logo here! -->
# MAGIC <img src="https://github.com/bionadmin/distrib/raw/main/core.png"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #Attempt to update the Gold schema without losing data.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Debug options

# COMMAND ----------

debug_mode = False # Include debug select and print statements
force_remount = False #Normally mount points are left alone if they exists. Setting this to true will replace the mountpoints.
force_rebuild_of_all_tables = False

# COMMAND ----------

# MAGIC %md
# MAGIC ####Some items we need...

# COMMAND ----------

from datetime import datetime
now = datetime.now()
tag = now.strftime("%Y%m%d%H%M%S")
print(tag)

useManagedTables = True
unmanagedArchiveCopies = 1
managedTablesKeepOldTableVersions = False
rebuildTableWhenColumnOrderChanges = True


destination_database_name="YAGoldModel"
spark.sql(f"DECLARE OR REPLACE VARIABLE var_destination_database_name STRING DEFAULT '{destination_database_name}'");



dest_base_path= "abfss://<Dest Container>@<Dest Storage Acct>.dfs.core.windows.net/<Dest Folder>/"

dest_additional_path="Gold/"
dest_additional_path_override = None
try:
    dest_additional_path_override=dbutils.secrets.get(scope="BionKeyvault",key="datalakestoragefoldergold")
    dest_additional_path = dest_additional_path_override or dest_additional_path
except:
    if debug_mode:
        print('No storage folder override found in keyvault. Using default value.')

dest_base_path=dest_base_path.replace("<Dest Folder>",dest_additional_path)
    
storage_account="skybitsgen2datalakestore"
storage_account_override=None
try:
    storage_account_override=dbutils.secrets.get(scope="BionKeyvault",key="datalakestorageaccount")
    storage_account = storage_account_override or storage_account
except:
    if debug_mode:
        print('No storage account override found in keyvault. Using default value.')

storage_container="Gold"
storage_container_override=None
try:
    storage_container_override=dbutils.secrets.get(scope="BionKeyvault",key="datalakestoragecontainergold")
    storage_container = storage_container_override or storage_container
except:
    if debug_mode:
        print('No storage container override found in keyvault. Using default value.')

dest_base_path=dest_base_path.replace("<Dest Storage Acct>",storage_account)
dest_base_path=dest_base_path.replace("<Dest Container>",storage_container) 
 
destination_file_folder="YAGoldModel"#set to None if no subfolder is desired. Include only internal slashes in the path.
if destination_file_folder==None or destination_file_folder=="":
    destination_database_default_path= dest_base_path
else:
    destination_database_default_path= dest_base_path + "/"+destination_file_folder
destination_filepath=destination_database_default_path+"/"

spark.sql(f"DECLARE OR REPLACE VARIABLE var_destination_database_default_path STRING DEFAULT '{destination_database_default_path}'");


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

# MAGIC
# MAGIC
# MAGIC %md 
# MAGIC ####Create Destination Database "YAGoldModel" 
# MAGIC (if it doesn't exist)<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC --Database:YAGoldModel
# MAGIC DECLARE OR REPLACE VARIABLE TMPSQL STRING DEFAULT "
# MAGIC CREATE CATALOG IF NOT EXISTS IDENTIFIER (var_destination_catalog_name);
# MAGIC ";
# MAGIC EXECUTE IMMEDIATE TMPSQL;
# MAGIC DECLARE OR REPLACE VARIABLE TMPSQL STRING DEFAULT "
# MAGIC CREATE DATABASE IF NOT EXISTS IDENTIFIER (var_destination_catalog_name||'.YAGoldModel') MANAGED LOCATION '"||var_destination_database_default_path||"'
# MAGIC ";
# MAGIC EXECUTE IMMEDIATE TMPSQL;

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

# Functions for altering tables. These functions behave differently 
# depending on if CDF is enabled on the table or not. 

# When CDF is NOT enabled (the default), a new table is created 
# (when there are schema changes) with the proper structure and 
# then existing data is copied from the old table into the new one.
# The old table is then deleted and the new table is renamed to the 
# old name. The final table should match the exact structure of the 
# Bion table when complete. 
# As such:
#   * New columns can be added 
#   * Deleted columns are removed
#   * Existing Columns can be renamed
#   * Existing Columns data types can be changed (assuming old values
#     can be cast to the new type)
#   * The order of the columns on the table be changed.
#   In all these cases the existing data will be retained (except for 
#   dropped columns, of course).
# This option gives the most flexibility when changing table structures, 
# but is limited in that history is not retained since a new table is created.

# When CDF IS enabled, a new table is NOT created. Instead, the table
# is updated in place in order to retain history. In this case 
# the behavior additionally depends on whether or not column mapping 
# is enabled for the table.
#   When Column mapping is NOT enabled (recommended):
#     * Only new columns are added. They are added at the end of the 
#     table. 
#     * If a column is renamed, then a NEW COLUMN is added and new 
#     data will flow into the new column, but old data will remain in 
#     the old column (which cannot be deleted). This is because columns
#     simply cannot be renamed or removed when column mapping is disabled.
#     * Dropped columns in the model will not be dropped in the table. 
#     * Columns cannot be reordered.
#     * Data types cannot be changed.
#
#   When Column mapping IS enabled (not recommended in most cases):
#     * New Columns can be added (still at the end of the table)
#     * Columns can be renamed.
#     * Columns can be dropped.
#     * Data types can NOT be changed.
#     * Columns can NOT be reordered.
#   In most cases this approach is not recommended as querying the table
#   for changes (the whole point of turning on CDF) is very difficult as
#   history must be fetched individually for each table structure and merged.
#   Complex routines must be written to fetch changes across schema changes.
#   There are also other limitations when column mapping is enabled that are beyond
#   the scope of these notes. 


def ChangeDataFeedEnabled(databaseName, tableName):
    return spark.sql(f'SHOW TBLPROPERTIES {databaseName}.{tableName}(delta.enableChangeDataFeed)').collect()[0][1]=='true'

def ColumnMappingEnabled(databaseName, tableName):
   return spark.sql(f'SHOW TBLPROPERTIES {databaseName}.{tableName}(delta.columnMapping.mode)').collect()[0][1]=='name'

def TableArraysAreEqual(old, new):
    if (len(old)!=len(new)):
        return False
    outerIndex=0
    for rowNew in new:
        innerIndex=0
        found = False
        for rowOld in old:
            if rowNew["col_name"]==rowOld["col_name"] and rowNew["data_type"]==rowOld["data_type"] and (rowNew["comment"] or "")==(rowOld["comment"] or ""):
                found=True
                break
            innerIndex=innerIndex+1
        if not found:
            if debug_mode:
                    print("Table structure has changed. Rebuilding table.")
            return False
        if rebuildTableWhenColumnOrderChanges and innerIndex!=outerIndex:
            if debug_mode:
                    print("Column Order has changed. Rebuilding table.")
            return False  
        outerIndex=outerIndex+1
    return True

def GetColumnByComment(arr,comment):
    if comment == "" or comment is None:
        return None
    for row in arr:
        if (row["comment"] is not None):
            if (comment in row["comment"]):
                return row
    return None

def GetColumnByName(arr, name):
    for row in arr:
        if (name == row["col_name"]):
            return row
    return None

def GetColumnByCommentThenName(arr, comment, name):
    row = GetColumnByComment(arr,comment)
    if row is None:
        row = GetColumnByName(arr,name)
    return row

def GenerateTableCreate(new, databaseName, tableName, partitions,location):

    stmt = "CREATE OR REPLACE TABLE "+databaseName+"."+tableName
    stmt=stmt+"\r\n("
    for row in new:
        stmt = stmt+"\r\n\t`"+row["col_name"]+"` "+row["data_type"]
        if row["comment"]:
            stmt = stmt +" COMMENT \""+row["comment"]+"\","
        else:
            stmt = stmt +","
    stmt=stmt[0:len(stmt)-1]#get rid of last trailing comma
    stmt=stmt+"\r\n)"  
    stmt=stmt+"\r\nUSING DELTA"
    if partitions is not None:
        if len(partitions)>0:
            stmt=stmt+"\r\nPARTITIONED BY("
            firstPartition=True
            for par in partitions:
                if not firstPartition:
                    stmt+=","
                firstPartition=False
                stmt+="`"+par+"`"
            stmt=stmt+")"
    if not useManagedTables:
        stmt=stmt+"\r\nLOCATION \""+location+"\""
    if debug_mode:
        print (stmt)
    return stmt

def GenerateTableCopy(old, new, databaseName, oldTableName, newTableName):
    stmt = "INSERT INTO "+databaseName+"."+newTableName
    stmt=stmt+"\r\n("
    for row in new:
        stmt = stmt+"\r\n\t`"+row["col_name"]+"`,"
    stmt=stmt[0:len(stmt)-1]#get rid of last trailing comma
    stmt=stmt+"\r\n)"   
    stmt=stmt+"SELECT "
    for row in new:
        oldRow = GetColumnByCommentThenName(old,row["comment"],row["col_name"])
        if oldRow is None:
            stmt=stmt+"\r\n\tNULL AS `"+row["col_name"]+"`,"
        else:
            stmt=stmt+"\r\n\tCAST(`"+oldRow["col_name"]+"` AS "+row["data_type"]+") AS `"+row["col_name"]+"`,"
    stmt=stmt[0:len(stmt)-1]#get rid of last trailing comma
    stmt=stmt+"\r\nFROM "+databaseName+"."+oldTableName
    if debug_mode:
        print (stmt)
    return stmt
 
def GenerateTableRename(databaseName, oldTableName, newTableName):
    stmt = "ALTER TABLE "+databaseName+"."+oldTableName +" RENAME TO "+newTableName
    if debug_mode:
        print (stmt)
    return stmt



def RemoveNonColumns(arr):
    returnArr = []
    for row in arr:
        if row["col_name"] == "# Partitioning":#Old way
            break  # we've gone past the columns. all done
        if row["col_name"] == "# Partition Information":#New Way
            break  # we've gone past the columns. all done
        if row["col_name"] == "# col_name":#New Way
            break  # we've gone past the columns. all done
        if row["data_type"] != "" and row["data_type"] is not None:
            returnArr.append(row)
    return returnArr
  
def TableExists(databaseName, tableName):
    table_list=spark.sql("show tables in "+databaseName+"")
    table_name=table_list.filter(table_list.tableName==tableName.lower()).collect()
    if table_name:
        return True
    else:
        return False
    
def UnmanagedRenameFolder(oldlocation,newlocation):
    if useManagedTables:
        return
    dbutils.fs.mv(oldlocation, newlocation,recurse = True)

def UnmanagedDropTable(databaseName, tableName):
    spark.sql("DROP TABLE "+databaseName+"."+tableName)

def ManagedDropTable(databaseName, tableName):
    spark.sql("DROP TABLE "+databaseName+"."+tableName)


def GetRenamedColumnsCDFOnly(old,new,tableName,columnMappingEnabled):
    renamedColumns = []
    for row in new:
        newColumnName = row["col_name"]
        oldColumn = GetColumnByCommentThenName(old,row["comment"],newColumnName)
        if oldColumn:
            if oldColumn["col_name"]!=newColumnName:
                if columnMappingEnabled:
                    renamedColumns.append({"oldColumnName":oldColumn["col_name"],"newColumnName":newColumnName})
                else:
                    print(f"Warning: Column rename column {newColumnName} for table {tableName} because CDF is enabled and column mapping is not. Column cannot be renamed")
    return renamedColumns 


def GetNewColumnsCDFOnly(old,new,tableName,columnMappingEnabled):
    newColumns = []
    for row in new:
        columnName = row["col_name"]
        oldColumn = GetColumnByName(old,row["col_name"])
        oldColumnByID = GetColumnByComment(old,row["comment"])
        if oldColumn is None and (oldColumnByID is None or not columnMappingEnabled):
            newColumns.append(row)
        if oldColumn is not None:
            if (oldColumn["data_type"]!=row["data_type"]):
                print(f"Warning: Column data type change detected on column {columnName} for table {tableName} but CDF is enabled. Column data type cannot be changed.")
    return newColumns    
    
def GetDroppedColumnsCDFOnly(old,new,tableName,columnMappingEnabled):
    droppedColumns = []
    for row in old:
        newColumn = GetColumnByName(new,row["col_name"])
        if newColumn is None:
            if columnMappingEnabled:
                droppedColumns.append(row)
            else:
                print(f"Warning: Cannot drop column {row['col_name']} for table {tableName} because CDF is enabled and column mapping is not.")
    return droppedColumns    

def UpdateTableStructureInPlace(old, new, databaseName, tableName):
    columnMappingEnabled = ColumnMappingEnabled(databaseName, tableName)
    newColumns = GetNewColumnsCDFOnly(old,new,tableName,columnMappingEnabled)
    renamedColumns = GetRenamedColumnsCDFOnly(old,new,tableName,columnMappingEnabled)
    droppedColumns = GetDroppedColumnsCDFOnly(old,new,tableName,columnMappingEnabled)
    
    if len(newColumns)==0 and len(droppedColumns)==0:
        return False
    
    for row in newColumns:
        cname = row["col_name"]
        ctype = row["data_type"]
        ccomment = row["comment"]
        
        alterstr = f"ALTER TABLE {databaseName}.`{tableName}` ADD COLUMN `{cname}` {ctype}"
        if ccomment:
            alterstr = alterstr+f" COMMENT \"{ccomment}\""
        print(alterstr)
        spark.sql(alterstr)

    for row in droppedColumns:
        cname = row["col_name"]
        ctype = row["data_type"]
        ccomment = row["comment"]
        alterstr = f"ALTER TABLE {databaseName}.`{tableName}` DROP COLUMN `{cname}`"
        print(alterstr)
        spark.sql(alterstr)

    for row in renamedColumns:
        oldname = row["oldColumnName"]
        newName = row["newColumnName"]
        alterstr = f"ALTER TABLE {databaseName}.`{tableName}` RENAME COLUMN `{oldname}` TO `{newName}`"
        print(alterstr)
        spark.sql(alterstr)

    return True

def UpdateTableStructure(old, new, databaseName, tableName, partitions, location, force):
    
    if (not force) and TableArraysAreEqual(old, new):
        if debug_mode:
            print ("Tables are the same. No need to update.")
        return

    cdfEnabled = ChangeDataFeedEnabled(databaseName, tableName)
    if cdfEnabled:
        if UpdateTableStructureInPlace(old,new,databaseName, tableName):
            return
        else:
            print("Warning: CDF is enabled. Some changes may not have been applied.")
            return

    stmt = GenerateTableCreate(new,databaseName, tableName+"_"+tag+"_TMP", partitions,location+"_"+tag+"_TMP")
    spark.sql(stmt)
    stmt = GenerateTableCopy(old,new,databaseName, tableName,tableName+"_"+tag+"_TMP")
    spark.sql(stmt)
    if useManagedTables:
        #With managed tables, the folder gets renamed with the table so this is all we need. Nice and simple.
        stmt = GenerateTableRename(databaseName,tableName, tableName+"_"+tag)
        spark.sql(stmt)
        stmt = GenerateTableRename(databaseName, tableName+"_"+tag+"_TMP",tableName)
        spark.sql(stmt)
        if not managedTablesKeepOldTableVersions:
            ManagedDropTable(databaseName, tableName+"_"+tag)
    if not useManagedTables:
        #with unmanaged, we need to rename the folder too so we don't have the new table's data sitting in a folder with a weird name...
        #basically since these are unmanaged we move the data around and then recreate the table which just updates the meta-data
        UnmanagedDropTable(databaseName,tableName)#drop our two tables.
        UnmanagedDropTable(databaseName,tableName+"_"+tag+"_TMP")#drop our two tables.
        UnmanagedRenameFolder(location,location+"_"+tag+"_PreviousVersion")#move the old version of the table's data out to a previous version folder.
        UnmanagedRenameFolder(location+"_"+tag+"_TMP",location)#move the new tables data into the folder with the basic table name
        stmt=GenerateTableCreate(new,databaseName, tableName, partitions,location)#create the table against the new path.
        if debug_mode:
            print(stmt)
        spark.sql(stmt)
        
def GetPartitions(collectedDescription):
    returnArr = []
    columns = RemoveNonColumns(collectedDescription)
    foundPartitionMarker = False
    newWay = False
    for row in collectedDescription:
        if row["col_name"] == "# Partitioning":  #older way
            foundPartitionMarker = True
            continue
        if row["col_name"] == "# Partition Information":  # newer way
            foundPartitionMarker = True
            newWay=True
            continue
        else:
            if not foundPartitionMarker:
                continue
        # if we made it this far, we are on the partitions section of the description.
        if row["col_name"] == "Not partitioned":
            return returnArr
        if newWay:
            potentialName = row["col_name"]
            if not potentialName.startswith("#"):
                for col in columns:
                    # confirm we are looking at a partition column
                    if col["col_name"].lower() == potentialName.lower():
                        returnArr.append(potentialName)
                        break
        else:
            if row["col_name"].startswith("Part "):  # partitions show up this way.
                potentialName = row[
                    "data_type"
                ]  # partition column names are in the type column.
                for col in columns:
                    # confirm we are looking at a partition column
                    if col["col_name"].lower() == potentialName.lower():
                        returnArr.append(potentialName)
                        break
    return returnArr


def StringArraysHaveSameElementsCI(one,two):
    if len(one)!=len(two):
        return False
    for strone in one:
        found = False
        for strtwo in two:
            if strone.lower()==strtwo.lower():
                found=True
                break
        if not found:
            return False
    return True
    
def RemoveOldArchiveCopies(tableName, filePath):
    if not useManagedTables:
        if unmanagedArchiveCopies>-1:#-1 means keep all copies!
            directoriesToParse=[]
            for fileinfo in dbutils.fs.ls(filePath):
                name = fileinfo.name
                if name.startswith(tableName) and len(name)==len(tableName)+32 and name.endswith("_PreviousVersion/"):
                    if debug_mode:
                        print (fileinfo.path)
                    directoriesToParse.append(fileinfo.path)
            directoriesToParse.sort(reverse=True)
            filesToKeep = unmanagedArchiveCopies
            for directory in directoriesToParse:
                if filesToKeep>0:
                    filesToKeep=filesToKeep-1
                    continue
                else:
                    if debug_mode:
                        print("Removing archive directory "+directory)
                    dbutils.fs.rm(directory,True)
                    
def TableIsManaged(databaseName, tableName):
    df = spark.sql("DESCRIBE EXTENDED "+databaseName+"."+tableName)
    arr = df.collect()
    foundDetailed = False
    for row in arr:
        if row["col_name"]=="# Detailed Table Information":
            foundDetailed=True
            continue
        if not foundDetailed:
            continue
        if row["col_name"]=="Type":
            if row["data_type"]=="MANAGED":
                return True
    return False

def ProcessTable(databaseName, tableName, comparisonArray, partitions, path):
    existingTableIsManaged=False
    tableExists = TableExists(databaseName,tableName)
    if tableExists:
        if debug_mode:
            print ("Table Exists")
        description = spark.sql("DESCRIBE "+databaseName+"."+tableName)
        oldpartitions = GetPartitions(description.collect())
        force = not StringArraysHaveSameElementsCI(oldpartitions,partitions)#rewrite table if partitions changed
        if force_rebuild_of_all_tables:
            force = True
        existingTableIsManaged = TableIsManaged(databaseName,tableName)
        if not force:  
            if (useManagedTables and not existingTableIsManaged) or (not useManagedTables and existingTableIsManaged):
                force = True
                if debug_mode:
                    if useManagedTables:
                        print("Converting tables from unmanaged to managed...")
                    else:
                        print("Converting tables from managed to unmanaged...")
        cols = RemoveNonColumns(description.collect())
        UpdateTableStructure(cols, comparisonArray, databaseName,tableName,partitions,path,force)
    else:
        if debug_mode:
            print ("Table Does not Exist. Creating Table...")
        stmt = GenerateTableCreate(comparisonArray,databaseName,tableName,partitions,path)
        spark.sql(stmt)

    if not existingTableIsManaged:
        RemoveOldArchiveCopies(tableName,destination_filepath)


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####Process table DimCustomer.

# COMMAND ----------

comparisonArray=[
	{"col_name":"CustomerKey", "data_type":"binary","comment":"[ID:{22856}]"}
	,{"col_name":"CustomerKeyInt", "data_type":"bigint","comment":"[ID:{22856INT}]"}
	,{"col_name":"CustomerID", "data_type":"int","comment":"[ID:{24640}]"}
	,{"col_name":"CustomerType", "data_type":"string","comment":"[ID:{35996}]"}
	,{"col_name":"CustomerFirstName", "data_type":"string","comment":"[ID:{92910}]"}
	,{"col_name":"CustomerLastName", "data_type":"string","comment":"[ID:{12657}]"}
	,{"col_name":"CustomerAccountNumber", "data_type":"string","comment":"[ID:{10754}]"}
	,{"col_name":"Audit_IsDeleted", "data_type":"boolean","comment":""}
	,{"col_name":"Audit_CreatedDateTime", "data_type":"timestamp","comment":""}
	,{"col_name":"Audit_ModifiedDateTime", "data_type":"timestamp","comment":""}
]
partitions=[]
path=destination_filepath+"DimCustomer"

ProcessTable('`'+destination_catalog_name+'`.'+destination_database_name,"DimCustomer",comparisonArray,partitions,path)




# COMMAND ----------

# MAGIC %md
# MAGIC ####Process table DimProduct.

# COMMAND ----------

comparisonArray=[
	{"col_name":"ProductKey", "data_type":"binary","comment":"[ID:{44295}]"}
	,{"col_name":"ProductKeyInt", "data_type":"bigint","comment":"[ID:{44295INT}]"}
	,{"col_name":"ProductID", "data_type":"bigint","comment":"[ID:{93526}]"}
	,{"col_name":"ProductName", "data_type":"string","comment":"[ID:{13126}]"}
	,{"col_name":"ProductNumber", "data_type":"string","comment":"[ID:{35407}]"}
	,{"col_name":"ProductColor", "data_type":"string","comment":"[ID:{47092}]"}
	,{"col_name":"Audit_IsDeleted", "data_type":"boolean","comment":""}
	,{"col_name":"Audit_CreatedDateTime", "data_type":"timestamp","comment":""}
	,{"col_name":"Audit_ModifiedDateTime", "data_type":"timestamp","comment":""}
]
partitions=[]
path=destination_filepath+"DimProduct"

ProcessTable('`'+destination_catalog_name+'`.'+destination_database_name,"DimProduct",comparisonArray,partitions,path)




# COMMAND ----------

# MAGIC %md
# MAGIC ####Process table FactSales.

# COMMAND ----------

comparisonArray=[
	{"col_name":"FactSalesKey", "data_type":"binary","comment":"[ID:{47369}]"}
	,{"col_name":"FactSalesKeyInt", "data_type":"bigint","comment":"[ID:{47369INT}]"}
	,{"col_name":"SalesOrderID", "data_type":"int","comment":"[ID:{74827}]"}
	,{"col_name":"SalesOrderDetailID", "data_type":"int","comment":"[ID:{44997}]"}
	,{"col_name":"LineTotal", "data_type":"decimal(38,6)","comment":"[ID:{20089}]"}
	,{"col_name":"CustomerKey", "data_type":"binary","comment":"[ID:{76463}]"}
	,{"col_name":"CustomerKeyInt", "data_type":"bigint","comment":"[ID:{76463INT}]"}
	,{"col_name":"ProductKey", "data_type":"binary","comment":"[ID:{64354}]"}
	,{"col_name":"ProductKeyInt", "data_type":"bigint","comment":"[ID:{64354INT}]"}
	,{"col_name":"Audit_IsDeleted", "data_type":"boolean","comment":""}
	,{"col_name":"Audit_CreatedDateTime", "data_type":"timestamp","comment":""}
	,{"col_name":"Audit_ModifiedDateTime", "data_type":"timestamp","comment":""}
]
partitions=[]
path=destination_filepath+"FactSales"

ProcessTable('`'+destination_catalog_name+'`.'+destination_database_name,"FactSales",comparisonArray,partitions,path)




# COMMAND ----------

# MAGIC %md
# MAGIC ####Process table Audit_RuntimeTableLog.

# COMMAND ----------

comparisonArray=[
	{"col_name":"GroupingID", "data_type":"string","comment":""}
    ,{"col_name":"StartDateTime", "data_type":"timestamp","comment":""}
	,{"col_name":"DatabaseName", "data_type":"string","comment":""}
    ,{"col_name":"TableName", "data_type":"string","comment":""}
    ,{"col_name":"TableStartTime", "data_type":"timestamp","comment":""}
    ,{"col_name":"TableEndTime", "data_type":"timestamp","comment":""}
    ,{"col_name":"TableDurationSeconds", "data_type":"bigint","comment":""}
    ,{"col_name":"RowsAffected", "data_type":"bigint","comment":""}
    ,{"col_name":"RowsInserted", "data_type":"bigint","comment":""}
    ,{"col_name":"RowsUpdated", "data_type":"bigint","comment":""}
    ,{"col_name":"RowsDeleted", "data_type":"bigint","comment":""}
    ,{"col_name":"RowsBefore", "data_type":"bigint","comment":""}
    ,{"col_name":"RowsAfter", "data_type":"bigint","comment":""}
    ,{"col_name":"Audit_Status", "data_type":"string","comment":""}
    ,{"col_name":"Audit_CreatedDateTime", "data_type":"timestamp","comment":""}
	,{"col_name":"Audit_ModifiedDateTime", "data_type":"timestamp","comment":""}
]
partitions=["StartDateTime","DatabaseName","TableName"]

path=destination_filepath+"Audit_RuntimeTableLog"

ProcessTable('`'+destination_catalog_name+'`.'+destination_database_name,"Audit_RuntimeTableLog",comparisonArray,partitions,path)

# COMMAND ----------

# Create primary keys when missing or when their ordered columns differ.
# Metadata query errors are intentionally allowed to fail this cell.

primary_key_specs = [
    {"table": 'DimCustomer', "constraint": 'DimCustomer_primary_key', "columns": [{"name": 'CustomerKey', "timeseries": False}]},
    {"table": 'DimProduct', "constraint": 'DimProduct_primary_key', "columns": [{"name": 'ProductKey', "timeseries": False}]},
    {"table": 'FactSales', "constraint": 'FactSales_primary_key', "columns": [{"name": 'FactSalesKey', "timeseries": False}]}
]

def _quote_identifier(identifier):
    return "`" + identifier.replace("`", "``") + "`"

def _sql_string(value):
    return "'" + value.replace("'", "''") + "'"

if primary_key_specs:
    catalog_identifier = _quote_identifier(destination_catalog_name)
    metadata_sql = f"""
        SELECT
            tc.table_name,
            kcu.column_name,
            kcu.ordinal_position
        FROM {catalog_identifier}.information_schema.table_constraints tc
        JOIN {catalog_identifier}.information_schema.key_column_usage kcu
          ON tc.constraint_catalog = kcu.constraint_catalog
         AND tc.constraint_schema = kcu.constraint_schema
         AND tc.constraint_name = kcu.constraint_name
         AND tc.table_catalog = kcu.table_catalog
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND lower(tc.table_catalog) = lower({_sql_string(destination_catalog_name)})
          AND lower(tc.table_schema) = lower({_sql_string(destination_database_name)})
        ORDER BY tc.table_name, kcu.ordinal_position
    """

    # Do this before altering any tables so missing metadata access fails before DDL.
    try:
        metadata_rows = spark.sql(metadata_sql).collect()
    except Exception as err:
        raise RuntimeError(
            "Unable to read primary-key metadata from "
            + destination_catalog_name
            + ".information_schema.table_constraints and key_column_usage. "
            "Primary-key constraint generation requires Unity Catalog metadata "
            "to be available and readable by this notebook. Enable/use Unity "
            "Catalog and grant the notebook identity access to the catalog "
            "metadata, or disable DestPKConstraintsEnabled for the affected "
            "tables in their table settings."
        ) from err

    existing_primary_keys = {}
    for row in metadata_rows:
        table_key = row["table_name"].lower()
        if table_key not in existing_primary_keys:
            existing_primary_keys[table_key] = {
                "columns": []
            }
        existing_primary_keys[table_key]["columns"].append(row["column_name"])

    for spec in primary_key_specs:
        table_identifier = (
            catalog_identifier + "."
            + _quote_identifier(destination_database_name) + "."
            + _quote_identifier(spec["table"])
        )
        for column in spec["columns"]:
            spark.sql(
                "ALTER TABLE " + table_identifier
                + " ALTER COLUMN " + _quote_identifier(column["name"])
                + " SET NOT NULL"
            )

    for spec in primary_key_specs:
        table_identifier = (
            catalog_identifier + "."
            + _quote_identifier(destination_database_name) + "."
            + _quote_identifier(spec["table"])
        )
        table_key = spec["table"].lower()
        expected_columns = [column["name"].lower() for column in spec["columns"]]
        existing_key = existing_primary_keys.get(table_key)

        if existing_key is not None:
            existing_columns = [column.lower() for column in existing_key["columns"]]
            if existing_columns == expected_columns:
                print("Primary key is already up to date on " + spec["table"])
                continue

            print("Replacing primary key on " + spec["table"] + " because its columns differ")
            spark.sql(
                "ALTER TABLE " + table_identifier
                + " DROP PRIMARY KEY IF EXISTS CASCADE"
            )

        ddl_columns = []
        for column in spec["columns"]:
            ddl_column = _quote_identifier(column["name"])
            if column["timeseries"]:
                ddl_column += " TIMESERIES"
            ddl_columns.append(ddl_column)

        add_primary_key_sql = (
            "ALTER TABLE " + table_identifier
            + " ADD CONSTRAINT " + _quote_identifier(spec["constraint"])
            + " PRIMARY KEY (" + ", ".join(ddl_columns) + ") NOT ENFORCED"
        )
        spark.sql(add_primary_key_sql)
        print("Created primary key on " + spec["table"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Update Table Comments
# MAGIC This section modifies the table comments for each table if the "Table Comment" setting has been added to each table.
# MAGIC If it does not already exist, add a TABLE setting called "Table Comment" (Text Area) to the CoreAccelerate model to control these comments for each table.
# MAGIC If you wish to maintain comments in CoreAccelerate, but don't wish to update the actual table comment in spark, add a MODEL setting called "Table Comments Disabled" (boolean) and check it. Doing this will keep these comments from updating here but will still allow the setting of these comments in CoreAccelerate if desired.
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'No Table Comments Defined'
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ##Enable CDF
# MAGIC CDF can be enabled for Core Accelerate tables either manually 
# MAGIC or via the table setting "Dest Change Data Feed Enabled". Once enabled, you must
# MAGIC manually disable it on a table if you want to turn it off again.
# MAGIC Please note the ramifications of turning on CDF as it relates to schema updates
# MAGIC as described above.
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'No CDF tables defined'
# MAGIC     

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####End of Notebook
# MAGIC
