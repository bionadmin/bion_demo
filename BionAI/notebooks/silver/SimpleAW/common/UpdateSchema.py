# Databricks notebook source
# MAGIC %md
# MAGIC # UpdateSchema — SimpleAW
# MAGIC
# MAGIC Creates tables that do not yet exist and adds new columns
# MAGIC to existing tables without reloading data.

# COMMAND ----------

# Unity Catalog variables
catalog = "silver"
database = "yasilvermodel"

# COMMAND ----------

# Ensure the Unity Catalog schema exists before creating tables
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")

# COMMAND ----------

# Create auxiliary/metadata tables
# Audit_RuntimeTableLog

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.Audit_RuntimeTableLog
    (
        TableName  STRING NOT NULL,
    RunTimestamp  TIMESTAMP,
    DurationSeconds  DOUBLE,
    RowsBefore  BIGINT,
    RowsAfter  BIGINT,
    RowsInserted  BIGINT,
    RowsUpdated  BIGINT,
    RowsDeleted  BIGINT
    )
    USING delta
""")

_aux_existing = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Audit_RuntimeTableLog").schema}

if 'tablename' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (TableName STRING)")

if 'runtimestamp' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (RunTimestamp TIMESTAMP)")

if 'durationseconds' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (DurationSeconds DOUBLE)")

if 'rowsbefore' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (RowsBefore BIGINT)")

if 'rowsafter' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (RowsAfter BIGINT)")

if 'rowsinserted' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (RowsInserted BIGINT)")

if 'rowsupdated' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (RowsUpdated BIGINT)")

if 'rowsdeleted' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Audit_RuntimeTableLog ADD COLUMNS (RowsDeleted BIGINT)")

# HighWatermark

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.HighWatermark
    (
        TableName  STRING NOT NULL,
    ColumnName  STRING NOT NULL,
    MaxValue  STRING
    )
    USING delta
""")

_aux_existing = {f.name.lower() for f in spark.table(f"{catalog}.{database}.HighWatermark").schema}

if 'tablename' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.HighWatermark ADD COLUMNS (TableName STRING)")

if 'columnname' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.HighWatermark ADD COLUMNS (ColumnName STRING)")

if 'maxvalue' not in _aux_existing:
    spark.sql(f"ALTER TABLE {catalog}.{database}.HighWatermark ADD COLUMNS (MaxValue STRING)")

# COMMAND ----------

# Schema update: Person_BusinessEntity
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Person_BusinessEntity (
  BusinessEntityID INT,
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Person_BusinessEntity that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Person_BusinessEntity").schema}
if 'businessentityid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntity ADD COLUMNS (BusinessEntityID INT)")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntity ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntity ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntity ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntity ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

# Schema update: Person_BusinessEntityAddress
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Person_BusinessEntityAddress (
  BusinessEntityID INT,
  AddressID INT,
  AddressTypeID INT,
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Person_BusinessEntityAddress that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Person_BusinessEntityAddress").schema}
if 'businessentityid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (BusinessEntityID INT)")
if 'addressid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (AddressID INT)")
if 'addresstypeid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (AddressTypeID INT)")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_BusinessEntityAddress ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

# Schema update: Person_Person
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Person_Person (
  BusinessEntityID INT,
  PersonType STRING,
  NameStyle BOOLEAN,
  Title STRING,
  FirstName STRING,
  MiddleName STRING,
  LastName STRING,
  Suffix STRING,
  EmailPromotion INT,
  AdditionalContactInfo STRING,
  Demographics STRING,
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Person_Person that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Person_Person").schema}
if 'businessentityid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (BusinessEntityID INT)")
if 'persontype' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (PersonType STRING)")
if 'namestyle' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (NameStyle BOOLEAN)")
if 'title' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (Title STRING)")
if 'firstname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (FirstName STRING)")
if 'middlename' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (MiddleName STRING)")
if 'lastname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (LastName STRING)")
if 'suffix' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (Suffix STRING)")
if 'emailpromotion' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (EmailPromotion INT)")
if 'additionalcontactinfo' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (AdditionalContactInfo STRING)")
if 'demographics' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (Demographics STRING)")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Person_Person ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

# Schema update: Production_Product
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Production_Product (
  ProductID INT,
  Name STRING,
  ProductNumber STRING,
  MakeFlag BOOLEAN,
  FinishedGoodsFlag BOOLEAN,
  Color STRING,
  SafetyStockLevel INT,
  ReorderPoint INT,
  StandardCost DECIMAL(19,4),
  ListPrice DECIMAL(19,4),
  Size STRING,
  SizeUnitMeasureCode STRING,
  WeightUnitMeasureCode STRING,
  Weight DECIMAL(8,2),
  DaysToManufacture INT,
  ProductLine STRING,
  Class STRING,
  Style STRING,
  ProductSubcategoryID INT,
  ProductModelID INT,
  SellStartDate TIMESTAMP,
  SellEndDate TIMESTAMP,
  DiscontinuedDate TIMESTAMP,
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Production_Product that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Production_Product").schema}
if 'productid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ProductID INT)")
if 'name' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Name STRING)")
if 'productnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ProductNumber STRING)")
if 'makeflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (MakeFlag BOOLEAN)")
if 'finishedgoodsflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (FinishedGoodsFlag BOOLEAN)")
if 'color' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Color STRING)")
if 'safetystocklevel' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (SafetyStockLevel INT)")
if 'reorderpoint' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ReorderPoint INT)")
if 'standardcost' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (StandardCost DECIMAL(19,4))")
if 'listprice' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ListPrice DECIMAL(19,4))")
if 'size' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Size STRING)")
if 'sizeunitmeasurecode' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (SizeUnitMeasureCode STRING)")
if 'weightunitmeasurecode' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (WeightUnitMeasureCode STRING)")
if 'weight' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Weight DECIMAL(8,2))")
if 'daystomanufacture' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (DaysToManufacture INT)")
if 'productline' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ProductLine STRING)")
if 'class' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Class STRING)")
if 'style' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Style STRING)")
if 'productsubcategoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ProductSubcategoryID INT)")
if 'productmodelid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ProductModelID INT)")
if 'sellstartdate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (SellStartDate TIMESTAMP)")
if 'sellenddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (SellEndDate TIMESTAMP)")
if 'discontinueddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (DiscontinuedDate TIMESTAMP)")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Production_Product ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

# Schema update: Sales_Customer
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Sales_Customer (
  CustomerID INT,
  PersonID INT,
  StoreID INT,
  TerritoryID INT,
  AccountNumber STRING,
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Sales_Customer that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Sales_Customer").schema}
if 'customerid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (CustomerID INT)")
if 'personid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (PersonID INT)")
if 'storeid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (StoreID INT)")
if 'territoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (TerritoryID INT)")
if 'accountnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (AccountNumber STRING)")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_Customer ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

# Schema update: Sales_SalesOrderDetail
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Sales_SalesOrderDetail (
  SalesOrderID INT,
  SalesOrderDetailID INT,
  CarrierTrackingNumber STRING,
  OrderQty INT,
  ProductID INT,
  SpecialOfferID INT,
  UnitPrice DECIMAL(19,4),
  UnitPriceDiscount DECIMAL(19,4),
  LineTotal DECIMAL(38,6),
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Sales_SalesOrderDetail that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Sales_SalesOrderDetail").schema}
if 'salesorderid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (SalesOrderID INT)")
if 'salesorderdetailid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (SalesOrderDetailID INT)")
if 'carriertrackingnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (CarrierTrackingNumber STRING)")
if 'orderqty' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (OrderQty INT)")
if 'productid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (ProductID INT)")
if 'specialofferid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (SpecialOfferID INT)")
if 'unitprice' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (UnitPrice DECIMAL(19,4))")
if 'unitpricediscount' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (UnitPriceDiscount DECIMAL(19,4))")
if 'linetotal' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (LineTotal DECIMAL(38,6))")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderDetail ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

# Schema update: Sales_SalesOrderHeader
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.Sales_SalesOrderHeader (
  SalesOrderID INT,
  RevisionNumber INT,
  OrderDate TIMESTAMP,
  DueDate TIMESTAMP,
  ShipDate TIMESTAMP,
  Status INT,
  OnlineOrderFlag BOOLEAN,
  SalesOrderNumber STRING,
  PurchaseOrderNumber STRING,
  AccountNumber STRING,
  CustomerID INT,
  SalesPersonID INT,
  TerritoryID INT,
  BillToAddressID INT,
  ShipToAddressID INT,
  ShipMethodID INT,
  CreditCardID INT,
  CreditCardApprovalCode STRING,
  CurrencyRateID INT,
  SubTotal DECIMAL(19,4),
  TaxAmt DECIMAL(19,4),
  Freight DECIMAL(19,4),
  TotalDue DECIMAL(19,4),
  Comment STRING,
  rowguid STRING,
  ModifiedDate TIMESTAMP,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP
  )
  USING delta
""")

# Add any new columns to Sales_SalesOrderHeader that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.Sales_SalesOrderHeader").schema}
if 'salesorderid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (SalesOrderID INT)")
if 'revisionnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (RevisionNumber INT)")
if 'orderdate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (OrderDate TIMESTAMP)")
if 'duedate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (DueDate TIMESTAMP)")
if 'shipdate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (ShipDate TIMESTAMP)")
if 'status' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (Status INT)")
if 'onlineorderflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (OnlineOrderFlag BOOLEAN)")
if 'salesordernumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (SalesOrderNumber STRING)")
if 'purchaseordernumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (PurchaseOrderNumber STRING)")
if 'accountnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (AccountNumber STRING)")
if 'customerid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (CustomerID INT)")
if 'salespersonid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (SalesPersonID INT)")
if 'territoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (TerritoryID INT)")
if 'billtoaddressid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (BillToAddressID INT)")
if 'shiptoaddressid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (ShipToAddressID INT)")
if 'shipmethodid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (ShipMethodID INT)")
if 'creditcardid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (CreditCardID INT)")
if 'creditcardapprovalcode' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (CreditCardApprovalCode STRING)")
if 'currencyrateid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (CurrencyRateID INT)")
if 'subtotal' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (SubTotal DECIMAL(19,4))")
if 'taxamt' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (TaxAmt DECIMAL(19,4))")
if 'freight' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (Freight DECIMAL(19,4))")
if 'totaldue' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (TotalDue DECIMAL(19,4))")
if 'comment' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (Comment STRING)")
if 'rowguid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (rowguid STRING)")
if 'modifieddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (ModifiedDate TIMESTAMP)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.Sales_SalesOrderHeader ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")

# COMMAND ----------

print("UpdateSchema complete.")