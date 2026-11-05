# Databricks notebook source
# MAGIC %md
# MAGIC <!-- your logo here! -->
# MAGIC <img src="https://github.com/bionadmin/distrib/raw/main/core.png"/>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Build Dim_Parent Gold Table

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
# MAGIC the name Dim_Parent.sql so that subsequent builds do not overwrite the query<br>
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
destination_table_name="Dim_Parent"
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
# MAGIC     'Dim_Parent' AS TableName,
# MAGIC     current_timestamp() AS TableStartTime,
# MAGIC     (select count(*) from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Dim_Parent')) AS RowsBefore
# MAGIC     WHERE NOT EXISTS (
# MAGIC         SELECT GroupingID from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_RuntimeTableLog') WHERE GroupingID = var_run_grouping_id 
# MAGIC         AND DatabaseName = var_destination_catalog_name||'.YAGoldModel' 
# MAGIC         AND TableName = 'Dim_Parent' 
# MAGIC     )
# MAGIC ) SRC;
# MAGIC
# MAGIC --In cases where we are restarting a specific GroupingID run...
# MAGIC UPDATE IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Audit_RuntimeTableLog')
# MAGIC   SET Audit_Status = 'Processing'
# MAGIC WHERE GroupingID = var_run_grouping_id
# MAGIC   AND DatabaseName = var_destination_catalog_name||'.YAGoldModel'
# MAGIC   AND TableName = 'Dim_Parent' 
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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW src_data_for_Dim_Parent AS
# MAGIC WITH SessionVariables AS (
# MAGIC     --There seems to be a bug where variables are not recognized in the identifier within a vew if the variable is not referenced
# MAGIC     --first elsewhere. This is the workaround so we can include these variables in the query if desired...
# MAGIC     --no need to actually use the values in this cte. The variables will work once "mentioned".
# MAGIC     SELECT var_source_catalog_name,var_destination_catalog_name 
# MAGIC )
# MAGIC SELECT
# MAGIC    Parent_Key
# MAGIC   ,AccountId
# MAGIC   ,SystemUniqueKey
# MAGIC   ,ParentId
# MAGIC   ,ContactId
# MAGIC   ,fp.Employee_Key
# MAGIC   ,OwnerId
# MAGIC   ,RecordTypeId
# MAGIC   ,fp.RecordTypeName
# MAGIC   ,Status
# MAGIC   ,UserRecord
# MAGIC   ,CampaignSource
# MAGIC   ,PiCampaign
# MAGIC   ,HDYHAU_Notes
# MAGIC   ,HowDidYouHearAboutUs
# MAGIC   ,EventDate
# MAGIC   ,EventLocation
# MAGIC   ,EventRegisteredFor
# MAGIC   ,EventTime
# MAGIC   ,EventType
# MAGIC   ,InquirySource
# MAGIC   ,DesiresTour
# MAGIC   ,IntakeScheduled
# MAGIC   ,ActualTourDate
# MAGIC   ,TourScheduled
# MAGIC   ,IntakeDate
# MAGIC   ,IntakeTime
# MAGIC   ,IntakeCompleted
# MAGIC   ,RegisteredDate
# MAGIC   ,RegistrationCompleted
# MAGIC   ,CandidatePotential
# MAGIC   ,ProfessionalLinkDate
# MAGIC   ,RelationshipWithSchool
# MAGIC   ,DivisionInterest
# MAGIC   ,DoNotContact
# MAGIC   ,DQDate
# MAGIC   ,DQReasonNotes
# MAGIC   ,InactiveDisqualifiedReason
# MAGIC   ,Region
# MAGIC   ,SchoolRegion
# MAGIC   ,SchoolInterestedIn
# MAGIC   ,SchoolSelected
# MAGIC   ,EnrollmentMonth
# MAGIC   ,EnrollmentYear
# MAGIC   ,LifecycleStatus
# MAGIC   ,PrimaryContact
# MAGIC   ,fp.FirstName
# MAGIC   ,fp.LastName
# MAGIC   ,fp.FullName
# MAGIC   ,AccountName
# MAGIC   ,ChildsFirstName
# MAGIC   ,ChildsLastName
# MAGIC   ,ChildsFirstName2
# MAGIC   ,ChildsLastName2
# MAGIC   ,ChildsFirstName3
# MAGIC   ,ChildsLastName3
# MAGIC   ,Phone
# MAGIC   ,fp.Email
# MAGIC   ,MailingStreet
# MAGIC   ,MailingCity
# MAGIC   ,MailingState
# MAGIC   ,MailingCountry
# MAGIC   ,MailingPostalCode
# MAGIC   ,MailingLatitude
# MAGIC   ,MailingLongitude
# MAGIC   ,GeneralNotes
# MAGIC   ,OriginallyCreatedDate
# MAGIC   ,fp.LastActivityDate
# MAGIC   ,fp.LastModifiedById
# MAGIC   ,fp.LastModifiedDate
# MAGIC   ,fp.CreatedById
# MAGIC   ,fp.CreatedDate
# MAGIC   ,fp.SystemModStamp
# MAGIC   ,fp.IsDeleted_Datalake
# MAGIC   ,fp.Audit_ModifiedDateTime
# MAGIC   ,current_timestamp() AS RIGHTNOW
# MAGIC FROM IDENTIFIER(var_destination_catalog_name||'.02_silver_tier_2.fusion_parent') fp
# MAGIC WHERE fp.Audit_ModifiedDateTime>CAST(var_max_audit_time AS TIMESTAMP)
# MAGIC
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
# MAGIC           t1.c0 AS ParentId,
# MAGIC           t1.c1 AS AccountId,
# MAGIC           t1.c2 AS SystemUniqueKey,
# MAGIC           t1.c3 AS ContactId,
# MAGIC           t1.c4 AS Employee_Key,
# MAGIC           t1.c5 AS OwnerId,
# MAGIC           t1.c6 AS RecordTypeId,
# MAGIC           t1.c7 AS RecordTypeName,
# MAGIC           t1.c8 AS Status,
# MAGIC           t1.c9 AS UserRecord,
# MAGIC           t1.c10 AS CampaignSource,
# MAGIC           t1.c11 AS PICampaign,
# MAGIC           t1.c12 AS HDYHAU_Notes,
# MAGIC           t1.c13 AS HowDidYouHearAboutUs,
# MAGIC           t1.c14 AS EventDate,
# MAGIC           t1.c15 AS EventLocation,
# MAGIC           t1.c16 AS EventRegisteredFor,
# MAGIC           t1.c17 AS EventTime,
# MAGIC           t1.c18 AS EventType,
# MAGIC           t1.c19 AS InquirySource,
# MAGIC           t1.c20 AS DesiresTour,
# MAGIC           t1.c21 AS IntakeScheduled,
# MAGIC           t1.c22 AS ActualTourDate,
# MAGIC           t1.c23 AS TourScheduled,
# MAGIC           t1.c24 AS IntakeDate,
# MAGIC           t1.c25 AS IntakeTime,
# MAGIC           t1.c26 AS IntakeCompleted,
# MAGIC           t1.c27 AS RegisteredDate,
# MAGIC           t1.c28 AS RegistrationCompleted,
# MAGIC           t1.c29 AS CandidatePotential,
# MAGIC           t1.c30 AS ProfessionalLinkDate,
# MAGIC           t1.c31 AS RelationshipWithSchool,
# MAGIC           t1.c32 AS DivisionInterest,
# MAGIC           t1.c33 AS DoNotContact,
# MAGIC           t1.c34 AS DQDate,
# MAGIC           t1.c35 AS DQReasonNotes,
# MAGIC           t1.c36 AS InactiveDisqualifiedReason,
# MAGIC           t1.c37 AS Region,
# MAGIC           t1.c38 AS SchoolRegion,
# MAGIC           t1.c39 AS SchoolInterestedIn,
# MAGIC           t1.c40 AS SchoolSelected,
# MAGIC           t1.c41 AS EnrollmentMonth,
# MAGIC           t1.c42 AS EnrollmentYear,
# MAGIC           t1.c43 AS LifecycleStatus,
# MAGIC           t1.c44 AS PrimaryContact,
# MAGIC           t1.c45 AS FirstName,
# MAGIC           t1.c46 AS Lastname,
# MAGIC           t1.c47 AS FullName,
# MAGIC           t1.c48 AS AccountName,
# MAGIC           t1.c49 AS ChildsFirstName,
# MAGIC           t1.c50 AS ChildsLastName,
# MAGIC           t1.c51 AS ChildsFirstName2,
# MAGIC           t1.c52 AS ChildsLastName2,
# MAGIC           t1.c53 AS ChildsFirstName3,
# MAGIC           t1.c54 AS ChildsLastName3,
# MAGIC           t1.c55 AS Phone,
# MAGIC           t1.c56 AS Email,
# MAGIC           t1.c57 AS MailingStreet,
# MAGIC           t1.c58 AS MailingCity,
# MAGIC           t1.c59 AS MailingState,
# MAGIC           t1.c60 AS MailingCountry,
# MAGIC           t1.c61 AS MailingPostalCode,
# MAGIC           t1.c62 AS MailingLatitude,
# MAGIC           t1.c63 AS MailingLongitude,
# MAGIC           t1.c64 AS GeneralNotes,
# MAGIC           t1.c65 AS OriginallyCreatedDate,
# MAGIC           t1.c66 AS LastActivityDate,
# MAGIC           t1.c67 AS LastModifiedById,
# MAGIC           t1.c68 AS LastModifiedDate,
# MAGIC           t1.c69 AS CreatedById,
# MAGIC           t1.c70 AS CreatedDate,
# MAGIC           t1.c71 AS SystemModStamp,
# MAGIC           t1.c72 AS IsDeleted_Datalake,
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
    display(spark.sql('select * from src_data_for_Dim_Parent LIMIT 1000'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Upsert Data

# COMMAND ----------

# MAGIC %sql
# MAGIC --Upsert data into table YAGoldModel.Dim_Parent
# MAGIC
# MAGIC
# MAGIC MERGE INTO IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Dim_Parent') DST
# MAGIC USING (
# MAGIC  SELECT
# MAGIC   *
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC           unhex(Parent_Key__HEXSTRING) AS Parent_Key,
# MAGIC           cast(conv(left(Parent_Key__HEXSTRING,15),16,10) as bigint) AS Parent_KeyInt,
# MAGIC           ParentId,
# MAGIC           AccountId,
# MAGIC           SystemUniqueKey,
# MAGIC           ContactId,
# MAGIC           Employee_Key,
# MAGIC           OwnerId,
# MAGIC           RecordTypeId,
# MAGIC           RecordTypeName,
# MAGIC           Status,
# MAGIC           UserRecord,
# MAGIC           CampaignSource,
# MAGIC           PICampaign,
# MAGIC           HDYHAU_Notes,
# MAGIC           HowDidYouHearAboutUs,
# MAGIC           EventDate,
# MAGIC           EventLocation,
# MAGIC           EventRegisteredFor,
# MAGIC           EventTime,
# MAGIC           EventType,
# MAGIC           InquirySource,
# MAGIC           DesiresTour,
# MAGIC           IntakeScheduled,
# MAGIC           ActualTourDate,
# MAGIC           TourScheduled,
# MAGIC           IntakeDate,
# MAGIC           IntakeTime,
# MAGIC           IntakeCompleted,
# MAGIC           RegisteredDate,
# MAGIC           RegistrationCompleted,
# MAGIC           CandidatePotential,
# MAGIC           ProfessionalLinkDate,
# MAGIC           RelationshipWithSchool,
# MAGIC           DivisionInterest,
# MAGIC           DoNotContact,
# MAGIC           DQDate,
# MAGIC           DQReasonNotes,
# MAGIC           InactiveDisqualifiedReason,
# MAGIC           Region,
# MAGIC           SchoolRegion,
# MAGIC           SchoolInterestedIn,
# MAGIC           SchoolSelected,
# MAGIC           EnrollmentMonth,
# MAGIC           EnrollmentYear,
# MAGIC           LifecycleStatus,
# MAGIC           PrimaryContact,
# MAGIC           FirstName,
# MAGIC           Lastname,
# MAGIC           FullName,
# MAGIC           AccountName,
# MAGIC           ChildsFirstName,
# MAGIC           ChildsLastName,
# MAGIC           ChildsFirstName2,
# MAGIC           ChildsLastName2,
# MAGIC           ChildsFirstName3,
# MAGIC           ChildsLastName3,
# MAGIC           Phone,
# MAGIC           Email,
# MAGIC           MailingStreet,
# MAGIC           MailingCity,
# MAGIC           MailingState,
# MAGIC           MailingCountry,
# MAGIC           MailingPostalCode,
# MAGIC           MailingLatitude,
# MAGIC           MailingLongitude,
# MAGIC           GeneralNotes,
# MAGIC           OriginallyCreatedDate,
# MAGIC           LastActivityDate,
# MAGIC           LastModifiedById,
# MAGIC           LastModifiedDate,
# MAGIC           CreatedById,
# MAGIC           CreatedDate,
# MAGIC           SystemModStamp,
# MAGIC           IsDeleted_Datalake,
# MAGIC   
# MAGIC           ROW_NUMBER() OVER (PARTITION BY Parent_Key__HEXSTRING ORDER BY Parent_Key__HEXSTRING) DUPLICATES_RN,--nothing good to order by.
# MAGIC   
# MAGIC           RIGHTNOW  
# MAGIC
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC           SHA2(
# MAGIC              CAST(IFNULL(ParentId,'NULL') AS STRING),256) AS Parent_Key__HEXSTRING,
# MAGIC           CAST(COALESCE(ParentId,'') AS STRING) AS ParentId,
# MAGIC           CAST(COALESCE(AccountId,'') AS STRING) AS AccountId,
# MAGIC           CAST(COALESCE(SystemUniqueKey,'') AS STRING) AS SystemUniqueKey,
# MAGIC           CAST(COALESCE(ContactId,'') AS STRING) AS ContactId,
# MAGIC           CAST(Employee_Key AS BINARY) AS Employee_Key,
# MAGIC           CAST(COALESCE(OwnerId,'') AS STRING) AS OwnerId,
# MAGIC           CAST(COALESCE(RecordTypeId,'') AS STRING) AS RecordTypeId,
# MAGIC           CAST(COALESCE(RecordTypeName,'') AS STRING) AS RecordTypeName,
# MAGIC           CAST(COALESCE(Status,'') AS STRING) AS Status,
# MAGIC           CAST(COALESCE(UserRecord,'') AS STRING) AS UserRecord,
# MAGIC           CAST(COALESCE(CampaignSource,'') AS STRING) AS CampaignSource,
# MAGIC           CAST(COALESCE(PICampaign,'') AS STRING) AS PICampaign,
# MAGIC           CAST(COALESCE(HDYHAU_Notes,'') AS STRING) AS HDYHAU_Notes,
# MAGIC           CAST(COALESCE(HowDidYouHearAboutUs,'') AS STRING) AS HowDidYouHearAboutUs,
# MAGIC           CAST(COALESCE(EventDate,'') AS STRING) AS EventDate,
# MAGIC           CAST(COALESCE(EventLocation,'') AS STRING) AS EventLocation,
# MAGIC           CAST(COALESCE(EventRegisteredFor,'') AS STRING) AS EventRegisteredFor,
# MAGIC           CAST(COALESCE(EventTime,'') AS STRING) AS EventTime,
# MAGIC           CAST(COALESCE(EventType,'') AS STRING) AS EventType,
# MAGIC           CAST(COALESCE(InquirySource,'') AS STRING) AS InquirySource,
# MAGIC           CAST(DesiresTour AS BOOLEAN) AS DesiresTour,
# MAGIC           CAST(IntakeScheduled AS BOOLEAN) AS IntakeScheduled,
# MAGIC           CAST(ActualTourDate AS DATE) AS ActualTourDate,
# MAGIC           CAST(TourScheduled AS BOOLEAN) AS TourScheduled,
# MAGIC           CAST(IntakeDate AS DATE) AS IntakeDate,
# MAGIC           CAST(COALESCE(IntakeTime,'') AS STRING) AS IntakeTime,
# MAGIC           CAST(IntakeCompleted AS BOOLEAN) AS IntakeCompleted,
# MAGIC           CAST(RegisteredDate AS DATE) AS RegisteredDate,
# MAGIC           CAST(RegistrationCompleted AS BOOLEAN) AS RegistrationCompleted,
# MAGIC           CAST(COALESCE(CandidatePotential,'') AS STRING) AS CandidatePotential,
# MAGIC           CAST(ProfessionalLinkDate AS DATE) AS ProfessionalLinkDate,
# MAGIC           CAST(COALESCE(RelationshipWithSchool,'') AS STRING) AS RelationshipWithSchool,
# MAGIC           CAST(COALESCE(DivisionInterest,'') AS STRING) AS DivisionInterest,
# MAGIC           CAST(DoNotContact AS BOOLEAN) AS DoNotContact,
# MAGIC           CAST(DQDate AS DATE) AS DQDate,
# MAGIC           CAST(COALESCE(DQReasonNotes,'') AS STRING) AS DQReasonNotes,
# MAGIC           CAST(COALESCE(InactiveDisqualifiedReason,'') AS STRING) AS InactiveDisqualifiedReason,
# MAGIC           CAST(COALESCE(Region,'') AS STRING) AS Region,
# MAGIC           CAST(COALESCE(SchoolRegion,'') AS STRING) AS SchoolRegion,
# MAGIC           CAST(COALESCE(SchoolInterestedIn,'') AS STRING) AS SchoolInterestedIn,
# MAGIC           CAST(COALESCE(SchoolSelected,'') AS STRING) AS SchoolSelected,
# MAGIC           CAST(COALESCE(EnrollmentMonth,'') AS STRING) AS EnrollmentMonth,
# MAGIC           CAST(COALESCE(EnrollmentYear,'') AS STRING) AS EnrollmentYear,
# MAGIC           CAST(COALESCE(LifecycleStatus,'') AS STRING) AS LifecycleStatus,
# MAGIC           CAST(PrimaryContact AS BOOLEAN) AS PrimaryContact,
# MAGIC           CAST(COALESCE(FirstName,'') AS STRING) AS FirstName,
# MAGIC           CAST(COALESCE(Lastname,'') AS STRING) AS Lastname,
# MAGIC           CAST(COALESCE(FullName,'') AS STRING) AS FullName,
# MAGIC           CAST(COALESCE(AccountName,'') AS STRING) AS AccountName,
# MAGIC           CAST(COALESCE(ChildsFirstName,'') AS STRING) AS ChildsFirstName,
# MAGIC           CAST(COALESCE(ChildsLastName,'') AS STRING) AS ChildsLastName,
# MAGIC           CAST(COALESCE(ChildsFirstName2,'') AS STRING) AS ChildsFirstName2,
# MAGIC           CAST(COALESCE(ChildsLastName2,'') AS STRING) AS ChildsLastName2,
# MAGIC           CAST(COALESCE(ChildsFirstName3,'') AS STRING) AS ChildsFirstName3,
# MAGIC           CAST(COALESCE(ChildsLastName3,'') AS STRING) AS ChildsLastName3,
# MAGIC           CAST(COALESCE(Phone,'') AS STRING) AS Phone,
# MAGIC           CAST(COALESCE(Email,'') AS STRING) AS Email,
# MAGIC           CAST(COALESCE(MailingStreet,'') AS STRING) AS MailingStreet,
# MAGIC           CAST(COALESCE(MailingCity,'') AS STRING) AS MailingCity,
# MAGIC           CAST(COALESCE(MailingState,'') AS STRING) AS MailingState,
# MAGIC           CAST(COALESCE(MailingCountry,'') AS STRING) AS MailingCountry,
# MAGIC           CAST(COALESCE(MailingPostalCode,'') AS STRING) AS MailingPostalCode,
# MAGIC           CAST(COALESCE(MailingLatitude,'') AS STRING) AS MailingLatitude,
# MAGIC           CAST(COALESCE(MailingLongitude,'') AS STRING) AS MailingLongitude,
# MAGIC           CAST(COALESCE(GeneralNotes,'') AS STRING) AS GeneralNotes,
# MAGIC           CAST(OriginallyCreatedDate AS DATE) AS OriginallyCreatedDate,
# MAGIC           CAST(LastActivityDate AS DATE) AS LastActivityDate,
# MAGIC           CAST(COALESCE(LastModifiedById,'') AS STRING) AS LastModifiedById,
# MAGIC           CAST(LastModifiedDate AS DATE) AS LastModifiedDate,
# MAGIC           CAST(COALESCE(CreatedById,'') AS STRING) AS CreatedById,
# MAGIC           CAST(CreatedDate AS DATE) AS CreatedDate,
# MAGIC           CAST(SystemModStamp AS DATE) AS SystemModStamp,
# MAGIC           CAST(IsDeleted_Datalake AS BOOLEAN) AS IsDeleted_Datalake,
# MAGIC     
# MAGIC           RIGHTNOW          
# MAGIC
# MAGIC     FROM src_data_for_Dim_Parent
# MAGIC     ) SRC1
# MAGIC
# MAGIC   ) SRC2 WHERE DUPLICATES_RN = 1
# MAGIC ) SRC
# MAGIC ON SRC.Parent_Key = DST.Parent_Key
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   DST.AccountId = SRC.AccountId,
# MAGIC   DST.SystemUniqueKey = SRC.SystemUniqueKey,
# MAGIC   DST.ContactId = SRC.ContactId,
# MAGIC   DST.Employee_Key = SRC.Employee_Key,
# MAGIC   DST.OwnerId = SRC.OwnerId,
# MAGIC   DST.RecordTypeId = SRC.RecordTypeId,
# MAGIC   DST.RecordTypeName = SRC.RecordTypeName,
# MAGIC   DST.Status = SRC.Status,
# MAGIC   DST.UserRecord = SRC.UserRecord,
# MAGIC   DST.CampaignSource = SRC.CampaignSource,
# MAGIC   DST.PICampaign = SRC.PICampaign,
# MAGIC   DST.HDYHAU_Notes = SRC.HDYHAU_Notes,
# MAGIC   DST.HowDidYouHearAboutUs = SRC.HowDidYouHearAboutUs,
# MAGIC   DST.EventDate = SRC.EventDate,
# MAGIC   DST.EventLocation = SRC.EventLocation,
# MAGIC   DST.EventRegisteredFor = SRC.EventRegisteredFor,
# MAGIC   DST.EventTime = SRC.EventTime,
# MAGIC   DST.EventType = SRC.EventType,
# MAGIC   DST.InquirySource = SRC.InquirySource,
# MAGIC   DST.DesiresTour = SRC.DesiresTour,
# MAGIC   DST.IntakeScheduled = SRC.IntakeScheduled,
# MAGIC   DST.ActualTourDate = SRC.ActualTourDate,
# MAGIC   DST.TourScheduled = SRC.TourScheduled,
# MAGIC   DST.IntakeDate = SRC.IntakeDate,
# MAGIC   DST.IntakeTime = SRC.IntakeTime,
# MAGIC   DST.IntakeCompleted = SRC.IntakeCompleted,
# MAGIC   DST.RegisteredDate = SRC.RegisteredDate,
# MAGIC   DST.RegistrationCompleted = SRC.RegistrationCompleted,
# MAGIC   DST.CandidatePotential = SRC.CandidatePotential,
# MAGIC   DST.ProfessionalLinkDate = SRC.ProfessionalLinkDate,
# MAGIC   DST.RelationshipWithSchool = SRC.RelationshipWithSchool,
# MAGIC   DST.DivisionInterest = SRC.DivisionInterest,
# MAGIC   DST.DoNotContact = SRC.DoNotContact,
# MAGIC   DST.DQDate = SRC.DQDate,
# MAGIC   DST.DQReasonNotes = SRC.DQReasonNotes,
# MAGIC   DST.InactiveDisqualifiedReason = SRC.InactiveDisqualifiedReason,
# MAGIC   DST.Region = SRC.Region,
# MAGIC   DST.SchoolRegion = SRC.SchoolRegion,
# MAGIC   DST.SchoolInterestedIn = SRC.SchoolInterestedIn,
# MAGIC   DST.SchoolSelected = SRC.SchoolSelected,
# MAGIC   DST.EnrollmentMonth = SRC.EnrollmentMonth,
# MAGIC   DST.EnrollmentYear = SRC.EnrollmentYear,
# MAGIC   DST.LifecycleStatus = SRC.LifecycleStatus,
# MAGIC   DST.PrimaryContact = SRC.PrimaryContact,
# MAGIC   DST.FirstName = SRC.FirstName,
# MAGIC   DST.Lastname = SRC.Lastname,
# MAGIC   DST.FullName = SRC.FullName,
# MAGIC   DST.AccountName = SRC.AccountName,
# MAGIC   DST.ChildsFirstName = SRC.ChildsFirstName,
# MAGIC   DST.ChildsLastName = SRC.ChildsLastName,
# MAGIC   DST.ChildsFirstName2 = SRC.ChildsFirstName2,
# MAGIC   DST.ChildsLastName2 = SRC.ChildsLastName2,
# MAGIC   DST.ChildsFirstName3 = SRC.ChildsFirstName3,
# MAGIC   DST.ChildsLastName3 = SRC.ChildsLastName3,
# MAGIC   DST.Phone = SRC.Phone,
# MAGIC   DST.Email = SRC.Email,
# MAGIC   DST.MailingStreet = SRC.MailingStreet,
# MAGIC   DST.MailingCity = SRC.MailingCity,
# MAGIC   DST.MailingState = SRC.MailingState,
# MAGIC   DST.MailingCountry = SRC.MailingCountry,
# MAGIC   DST.MailingPostalCode = SRC.MailingPostalCode,
# MAGIC   DST.MailingLatitude = SRC.MailingLatitude,
# MAGIC   DST.MailingLongitude = SRC.MailingLongitude,
# MAGIC   DST.GeneralNotes = SRC.GeneralNotes,
# MAGIC   DST.OriginallyCreatedDate = SRC.OriginallyCreatedDate,
# MAGIC   DST.LastActivityDate = SRC.LastActivityDate,
# MAGIC   DST.LastModifiedById = SRC.LastModifiedById,
# MAGIC   DST.LastModifiedDate = SRC.LastModifiedDate,
# MAGIC   DST.CreatedById = SRC.CreatedById,
# MAGIC   DST.CreatedDate = SRC.CreatedDate,
# MAGIC   DST.SystemModStamp = SRC.SystemModStamp,
# MAGIC   DST.IsDeleted_Datalake = SRC.IsDeleted_Datalake,
# MAGIC   DST.Audit_IsDeleted=false,
# MAGIC   DST.Audit_RowIsInferred=false,
# MAGIC  
# MAGIC   DST.Audit_ModifiedDateTime = SRC.RIGHTNOW
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     Parent_Key,
# MAGIC     Parent_KeyInt,
# MAGIC     ParentId,
# MAGIC     AccountId,
# MAGIC     SystemUniqueKey,
# MAGIC     ContactId,
# MAGIC     Employee_Key,
# MAGIC     OwnerId,
# MAGIC     RecordTypeId,
# MAGIC     RecordTypeName,
# MAGIC     Status,
# MAGIC     UserRecord,
# MAGIC     CampaignSource,
# MAGIC     PICampaign,
# MAGIC     HDYHAU_Notes,
# MAGIC     HowDidYouHearAboutUs,
# MAGIC     EventDate,
# MAGIC     EventLocation,
# MAGIC     EventRegisteredFor,
# MAGIC     EventTime,
# MAGIC     EventType,
# MAGIC     InquirySource,
# MAGIC     DesiresTour,
# MAGIC     IntakeScheduled,
# MAGIC     ActualTourDate,
# MAGIC     TourScheduled,
# MAGIC     IntakeDate,
# MAGIC     IntakeTime,
# MAGIC     IntakeCompleted,
# MAGIC     RegisteredDate,
# MAGIC     RegistrationCompleted,
# MAGIC     CandidatePotential,
# MAGIC     ProfessionalLinkDate,
# MAGIC     RelationshipWithSchool,
# MAGIC     DivisionInterest,
# MAGIC     DoNotContact,
# MAGIC     DQDate,
# MAGIC     DQReasonNotes,
# MAGIC     InactiveDisqualifiedReason,
# MAGIC     Region,
# MAGIC     SchoolRegion,
# MAGIC     SchoolInterestedIn,
# MAGIC     SchoolSelected,
# MAGIC     EnrollmentMonth,
# MAGIC     EnrollmentYear,
# MAGIC     LifecycleStatus,
# MAGIC     PrimaryContact,
# MAGIC     FirstName,
# MAGIC     Lastname,
# MAGIC     FullName,
# MAGIC     AccountName,
# MAGIC     ChildsFirstName,
# MAGIC     ChildsLastName,
# MAGIC     ChildsFirstName2,
# MAGIC     ChildsLastName2,
# MAGIC     ChildsFirstName3,
# MAGIC     ChildsLastName3,
# MAGIC     Phone,
# MAGIC     Email,
# MAGIC     MailingStreet,
# MAGIC     MailingCity,
# MAGIC     MailingState,
# MAGIC     MailingCountry,
# MAGIC     MailingPostalCode,
# MAGIC     MailingLatitude,
# MAGIC     MailingLongitude,
# MAGIC     GeneralNotes,
# MAGIC     OriginallyCreatedDate,
# MAGIC     LastActivityDate,
# MAGIC     LastModifiedById,
# MAGIC     LastModifiedDate,
# MAGIC     CreatedById,
# MAGIC     CreatedDate,
# MAGIC     SystemModStamp,
# MAGIC     IsDeleted_Datalake,
# MAGIC     Audit_IsDeleted,
# MAGIC     Audit_RowIsInferred,
# MAGIC  
# MAGIC     Audit_CreatedDateTime,
# MAGIC     Audit_ModifiedDateTime
# MAGIC     
# MAGIC  )
# MAGIC  VALUES(
# MAGIC    SRC.Parent_Key,
# MAGIC    SRC.Parent_KeyInt,    
# MAGIC    SRC.ParentId,
# MAGIC    SRC.AccountId,
# MAGIC    SRC.SystemUniqueKey,
# MAGIC    SRC.ContactId,
# MAGIC    SRC.Employee_Key,
# MAGIC    SRC.OwnerId,
# MAGIC    SRC.RecordTypeId,
# MAGIC    SRC.RecordTypeName,
# MAGIC    SRC.Status,
# MAGIC    SRC.UserRecord,
# MAGIC    SRC.CampaignSource,
# MAGIC    SRC.PICampaign,
# MAGIC    SRC.HDYHAU_Notes,
# MAGIC    SRC.HowDidYouHearAboutUs,
# MAGIC    SRC.EventDate,
# MAGIC    SRC.EventLocation,
# MAGIC    SRC.EventRegisteredFor,
# MAGIC    SRC.EventTime,
# MAGIC    SRC.EventType,
# MAGIC    SRC.InquirySource,
# MAGIC    SRC.DesiresTour,
# MAGIC    SRC.IntakeScheduled,
# MAGIC    SRC.ActualTourDate,
# MAGIC    SRC.TourScheduled,
# MAGIC    SRC.IntakeDate,
# MAGIC    SRC.IntakeTime,
# MAGIC    SRC.IntakeCompleted,
# MAGIC    SRC.RegisteredDate,
# MAGIC    SRC.RegistrationCompleted,
# MAGIC    SRC.CandidatePotential,
# MAGIC    SRC.ProfessionalLinkDate,
# MAGIC    SRC.RelationshipWithSchool,
# MAGIC    SRC.DivisionInterest,
# MAGIC    SRC.DoNotContact,
# MAGIC    SRC.DQDate,
# MAGIC    SRC.DQReasonNotes,
# MAGIC    SRC.InactiveDisqualifiedReason,
# MAGIC    SRC.Region,
# MAGIC    SRC.SchoolRegion,
# MAGIC    SRC.SchoolInterestedIn,
# MAGIC    SRC.SchoolSelected,
# MAGIC    SRC.EnrollmentMonth,
# MAGIC    SRC.EnrollmentYear,
# MAGIC    SRC.LifecycleStatus,
# MAGIC    SRC.PrimaryContact,
# MAGIC    SRC.FirstName,
# MAGIC    SRC.Lastname,
# MAGIC    SRC.FullName,
# MAGIC    SRC.AccountName,
# MAGIC    SRC.ChildsFirstName,
# MAGIC    SRC.ChildsLastName,
# MAGIC    SRC.ChildsFirstName2,
# MAGIC    SRC.ChildsLastName2,
# MAGIC    SRC.ChildsFirstName3,
# MAGIC    SRC.ChildsLastName3,
# MAGIC    SRC.Phone,
# MAGIC    SRC.Email,
# MAGIC    SRC.MailingStreet,
# MAGIC    SRC.MailingCity,
# MAGIC    SRC.MailingState,
# MAGIC    SRC.MailingCountry,
# MAGIC    SRC.MailingPostalCode,
# MAGIC    SRC.MailingLatitude,
# MAGIC    SRC.MailingLongitude,
# MAGIC    SRC.GeneralNotes,
# MAGIC    SRC.OriginallyCreatedDate,
# MAGIC    SRC.LastActivityDate,
# MAGIC    SRC.LastModifiedById,
# MAGIC    SRC.LastModifiedDate,
# MAGIC    SRC.CreatedById,
# MAGIC    SRC.CreatedDate,
# MAGIC    SRC.SystemModStamp,
# MAGIC    SRC.IsDeleted_Datalake,
# MAGIC    false,
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
# MAGIC --Delete data from the YAGoldModel.Dim_Parent table
# MAGIC
# MAGIC
# MAGIC MERGE INTO IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Dim_Parent') DST
# MAGIC USING (
# MAGIC  SELECT 
# MAGIC     ParentId,--primary or alternate key
# MAGIC     current_timestamp() AS RIGHTNOW--leave this.
# MAGIC  FROM IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Dim_Parent') InTable 
# MAGIC  LEFT ANTI JOIN src_data_for_Dim_Parent InSource
# MAGIC ON 1=1
# MAGIC  AND (InTable.ParentId=InSource.ParentId)
# MAGIC ) SRC
# MAGIC ON
# MAGIC 1=1
# MAGIC  AND (DST.ParentId=SRC.ParentId)
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

rowsupserteddf.createOrReplaceTempView("tmp_YAGoldModel_Dim_Parent_upserted");
if ("rowsdeleteddf" in locals()):
    rowsdeleteddf.createOrReplaceTempView("tmp_YAGoldModel_Dim_Parent_deleted")
else:
    #cannot make another view off of rowsupserteddf as this will fail when attempting to join two views from the same dataframe. Thus:
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW tmp_YAGoldModel_Dim_Parent_deleted AS (SELECT * FROM tmp_YAGoldModel_Dim_Parent_upserted)")
    

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
# MAGIC     FROM tmp_YAGoldModel_Dim_Parent_upserted u
# MAGIC     CROSS JOIN tmp_YAGoldModel_Dim_Parent_deleted d
# MAGIC   ),
# MAGIC   RowsInserted = COALESCE(RowsInserted,0)+(select num_inserted_rows from tmp_YAGoldModel_Dim_Parent_upserted),
# MAGIC   RowsUpdated = COALESCE(RowsUpdated,0)+(select num_updated_rows from tmp_YAGoldModel_Dim_Parent_upserted),
# MAGIC   RowsDeleted = COALESCE(RowsDeleted,0)+(select num_deleted_rows from tmp_YAGoldModel_Dim_Parent_deleted),
# MAGIC   RowsAfter = (select count(*) from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Dim_Parent')),
# MAGIC   Audit_Status = 'Completed',
# MAGIC   Audit_ModifiedDateTime = current_timestamp()
# MAGIC WHERE
# MAGIC   GroupingID = var_run_grouping_id
# MAGIC   AND DatabaseName = var_destination_catalog_name||'.YAGoldModel'
# MAGIC   AND TableName = 'Dim_Parent'
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
    display(spark.sql("select * from IDENTIFIER(var_destination_catalog_name||'.YAGoldModel'||'.Dim_Parent') ORDER BY Audit_ModifiedDateTime DESC LIMIT 1000"))

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
