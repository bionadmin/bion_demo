# Databricks notebook source
# MAGIC %md
# MAGIC # UpdateSchema — SimpleAWGold
# MAGIC
# MAGIC Creates tables that do not yet exist and adds new columns
# MAGIC to existing tables without reloading data.

# COMMAND ----------

# Unity Catalog variables
catalog = "gold"
database = "yagoldmodel2"

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

# COMMAND ----------

# Schema update: DimCustomer
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.DimCustomer (
  `CustomerKey` BINARY,
  `CustomerID` INT,
  `AccountNumber` STRING,
  `PersonID` INT,
  `StoreID` INT,
  `TerritoryID` INT,
  `PersonType` STRING,
  `Title` STRING,
  `FirstName` STRING,
  `MiddleName` STRING,
  `LastName` STRING,
  `Suffix` STRING,
  `FullName` STRING,
  `EmailPromotion` INT,
  `CustomerKey_Id` BIGINT,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP,
  ChangeHash BINARY
  )
  USING delta
""")

# Add any new columns to DimCustomer that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.DimCustomer").schema}
if 'customerkey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`CustomerKey` BINARY)")
if 'customerid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`CustomerID` INT)")
if 'accountnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`AccountNumber` STRING)")
if 'personid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`PersonID` INT)")
if 'storeid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`StoreID` INT)")
if 'territoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`TerritoryID` INT)")
if 'persontype' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`PersonType` STRING)")
if 'title' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`Title` STRING)")
if 'firstname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`FirstName` STRING)")
if 'middlename' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`MiddleName` STRING)")
if 'lastname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`LastName` STRING)")
if 'suffix' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`Suffix` STRING)")
if 'fullname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`FullName` STRING)")
if 'emailpromotion' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`EmailPromotion` INT)")
if 'customerkey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (`CustomerKey_Id` BIGINT)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")
if 'changehash' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimCustomer ADD COLUMNS (ChangeHash BINARY)")

# COMMAND ----------

# Schema update: DimDate
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.DimDate (
  `DateKey` INT,
  `FullDate` DATE,
  `Year` INT,
  `Quarter` INT,
  `Month` INT,
  `MonthName` STRING,
  `Day` INT,
  `DayOfWeek` INT,
  `DayName` STRING,
  `IsWeekend` BOOLEAN,
  `WeekOfYear` INT,
  `YearMonth` INT,
  `SurrogateKey` BINARY,
  `SurrogateKey_Id` BIGINT,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP,
  ChangeHash BINARY
  )
  USING delta
""")

# Add any new columns to DimDate that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.DimDate").schema}
if 'datekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`DateKey` INT)")
if 'fulldate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`FullDate` DATE)")
if 'year' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`Year` INT)")
if 'quarter' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`Quarter` INT)")
if 'month' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`Month` INT)")
if 'monthname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`MonthName` STRING)")
if 'day' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`Day` INT)")
if 'dayofweek' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`DayOfWeek` INT)")
if 'dayname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`DayName` STRING)")
if 'isweekend' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`IsWeekend` BOOLEAN)")
if 'weekofyear' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`WeekOfYear` INT)")
if 'yearmonth' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`YearMonth` INT)")
if 'surrogatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`SurrogateKey` BINARY)")
if 'surrogatekey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (`SurrogateKey_Id` BIGINT)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")
if 'changehash' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimDate ADD COLUMNS (ChangeHash BINARY)")

# COMMAND ----------

# Schema update: DimProduct
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.DimProduct (
  `ProductKey` BINARY,
  `ProductID` INT,
  `ProductName` STRING,
  `ProductNumber` STRING,
  `MakeFlag` BOOLEAN,
  `FinishedGoodsFlag` BOOLEAN,
  `Color` STRING,
  `Size` STRING,
  `SizeUnitMeasureCode` STRING,
  `Weight` DECIMAL(8,2),
  `WeightUnitMeasureCode` STRING,
  `ProductLine` STRING,
  `Class` STRING,
  `Style` STRING,
  `StandardCost` DECIMAL(19,4),
  `ListPrice` DECIMAL(19,4),
  `DaysToManufacture` INT,
  `ProductSubcategoryID` INT,
  `ProductModelID` INT,
  `SellStartDate` DATE,
  `SellEndDate` DATE,
  `DiscontinuedDate` DATE,
  `ProductKey_Id` BIGINT,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP,
  ChangeHash BINARY
  )
  USING delta
""")

# Add any new columns to DimProduct that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.DimProduct").schema}
if 'productkey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductKey` BINARY)")
if 'productid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductID` INT)")
if 'productname' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductName` STRING)")
if 'productnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductNumber` STRING)")
if 'makeflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`MakeFlag` BOOLEAN)")
if 'finishedgoodsflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`FinishedGoodsFlag` BOOLEAN)")
if 'color' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`Color` STRING)")
if 'size' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`Size` STRING)")
if 'sizeunitmeasurecode' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`SizeUnitMeasureCode` STRING)")
if 'weight' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`Weight` DECIMAL(8,2))")
if 'weightunitmeasurecode' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`WeightUnitMeasureCode` STRING)")
if 'productline' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductLine` STRING)")
if 'class' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`Class` STRING)")
if 'style' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`Style` STRING)")
if 'standardcost' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`StandardCost` DECIMAL(19,4))")
if 'listprice' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ListPrice` DECIMAL(19,4))")
if 'daystomanufacture' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`DaysToManufacture` INT)")
if 'productsubcategoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductSubcategoryID` INT)")
if 'productmodelid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductModelID` INT)")
if 'sellstartdate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`SellStartDate` DATE)")
if 'sellenddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`SellEndDate` DATE)")
if 'discontinueddate' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`DiscontinuedDate` DATE)")
if 'productkey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (`ProductKey_Id` BIGINT)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")
if 'changehash' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.DimProduct ADD COLUMNS (ChangeHash BINARY)")

# COMMAND ----------

# Schema update: FactSalesOrder
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.FactSalesOrder (
  `SalesOrderKey` BINARY,
  `SalesOrderID` INT,
  `SalesOrderNumber` STRING,
  `PurchaseOrderNumber` STRING,
  `CustomerKey` BINARY,
  `OrderDateKey` INT,
  `DueDateKey` INT,
  `ShipDateKey` INT,
  `RevisionNumber` INT,
  `Status` INT,
  `OnlineOrderFlag` BOOLEAN,
  `TerritoryID` INT,
  `SalesPersonID` INT,
  `ShipMethodID` INT,
  `SubTotal` DECIMAL(19,4),
  `TaxAmt` DECIMAL(19,4),
  `Freight` DECIMAL(19,4),
  `TotalDue` DECIMAL(19,4),
  `SalesOrderKey_Id` BIGINT,
  `CustomerKey_Id` BIGINT,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP,
  ChangeHash BINARY
  )
  USING delta
""")

# Add any new columns to FactSalesOrder that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.FactSalesOrder").schema}
if 'salesorderkey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`SalesOrderKey` BINARY)")
if 'salesorderid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`SalesOrderID` INT)")
if 'salesordernumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`SalesOrderNumber` STRING)")
if 'purchaseordernumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`PurchaseOrderNumber` STRING)")
if 'customerkey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`CustomerKey` BINARY)")
if 'orderdatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`OrderDateKey` INT)")
if 'duedatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`DueDateKey` INT)")
if 'shipdatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`ShipDateKey` INT)")
if 'revisionnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`RevisionNumber` INT)")
if 'status' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`Status` INT)")
if 'onlineorderflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`OnlineOrderFlag` BOOLEAN)")
if 'territoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`TerritoryID` INT)")
if 'salespersonid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`SalesPersonID` INT)")
if 'shipmethodid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`ShipMethodID` INT)")
if 'subtotal' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`SubTotal` DECIMAL(19,4))")
if 'taxamt' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`TaxAmt` DECIMAL(19,4))")
if 'freight' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`Freight` DECIMAL(19,4))")
if 'totaldue' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`TotalDue` DECIMAL(19,4))")
if 'salesorderkey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`SalesOrderKey_Id` BIGINT)")
if 'customerkey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (`CustomerKey_Id` BIGINT)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")
if 'changehash' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrder ADD COLUMNS (ChangeHash BINARY)")

# COMMAND ----------

# Schema update: FactSalesOrderLine
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{database}.FactSalesOrderLine (
  `SalesOrderLineKey` BINARY,
  `SalesOrderID` INT,
  `SalesOrderDetailID` INT,
  `SalesOrderNumber` STRING,
  `CustomerKey` BINARY,
  `ProductKey` BINARY,
  `OrderDateKey` INT,
  `DueDateKey` INT,
  `ShipDateKey` INT,
  `Status` INT,
  `OnlineOrderFlag` BOOLEAN,
  `TerritoryID` INT,
  `SalesPersonID` INT,
  `SpecialOfferID` INT,
  `CarrierTrackingNumber` STRING,
  `OrderQty` INT,
  `UnitPrice` DECIMAL(19,4),
  `UnitPriceDiscount` DECIMAL(19,4),
  `LineTotal` DECIMAL(38,6),
  `SalesOrderLineKey_Id` BIGINT,
  `CustomerKey_Id` BIGINT,
  `ProductKey_Id` BIGINT,
  Audit_CreatedDateTime TIMESTAMP,
  Audit_ModifiedDateTime TIMESTAMP,
  ChangeHash BINARY
  )
  USING delta
""")

# Add any new columns to FactSalesOrderLine that don't exist yet
_existing_cols = {f.name.lower() for f in spark.table(f"{catalog}.{database}.FactSalesOrderLine").schema}
if 'salesorderlinekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SalesOrderLineKey` BINARY)")
if 'salesorderid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SalesOrderID` INT)")
if 'salesorderdetailid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SalesOrderDetailID` INT)")
if 'salesordernumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SalesOrderNumber` STRING)")
if 'customerkey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`CustomerKey` BINARY)")
if 'productkey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`ProductKey` BINARY)")
if 'orderdatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`OrderDateKey` INT)")
if 'duedatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`DueDateKey` INT)")
if 'shipdatekey' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`ShipDateKey` INT)")
if 'status' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`Status` INT)")
if 'onlineorderflag' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`OnlineOrderFlag` BOOLEAN)")
if 'territoryid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`TerritoryID` INT)")
if 'salespersonid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SalesPersonID` INT)")
if 'specialofferid' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SpecialOfferID` INT)")
if 'carriertrackingnumber' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`CarrierTrackingNumber` STRING)")
if 'orderqty' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`OrderQty` INT)")
if 'unitprice' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`UnitPrice` DECIMAL(19,4))")
if 'unitpricediscount' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`UnitPriceDiscount` DECIMAL(19,4))")
if 'linetotal' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`LineTotal` DECIMAL(38,6))")
if 'salesorderlinekey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`SalesOrderLineKey_Id` BIGINT)")
if 'customerkey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`CustomerKey_Id` BIGINT)")
if 'productkey_id' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (`ProductKey_Id` BIGINT)")
if 'audit_createddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (Audit_CreatedDateTime TIMESTAMP)")
if 'audit_modifieddatetime' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (Audit_ModifiedDateTime TIMESTAMP)")
if 'changehash' not in _existing_cols:
    spark.sql(f"ALTER TABLE {catalog}.{database}.FactSalesOrderLine ADD COLUMNS (ChangeHash BINARY)")

# COMMAND ----------

print("UpdateSchema complete.")