# Databricks notebook source
# MAGIC %md
# MAGIC # InferMissingRows — SimpleAWGold
# MAGIC
# MAGIC Final post-step of a model run: inserts dimension rows for the keys that fact
# MAGIC notebooks staged in `<Dimension>_InferStage` and found missing, so that
# MAGIC fact-dimension joins never produce NULLs. Run by RunAllNotebooks after every
# MAGIC table notebook has finished.

# COMMAND ----------

catalog = "gold"
database = "yagoldmodel2"

# COMMAND ----------

_infer_errors: list[str] = []

# COMMAND ----------

# DimCustomer: insert the staged members that are still missing, then clear the stage
try:
    spark.sql(f"""
        INSERT INTO {catalog}.{database}.DimCustomer
          (`CustomerKey`, `CustomerKey_Id`, `CustomerID`, Audit_CreatedDateTime, Audit_ModifiedDateTime, Audit_IsInferred, `Audit_IsUnknown`)
        SELECT s.`CustomerKey`, MAX(s.`CustomerKey_Id`), MAX(s.`CustomerID`), current_timestamp(), current_timestamp(), true, MAX(s.`Audit_IsUnknown`)
        FROM {catalog}.{database}.DimCustomer_InferStage s
        LEFT ANTI JOIN {catalog}.{database}.DimCustomer d ON s.`CustomerKey` = d.`CustomerKey`
        GROUP BY s.`CustomerKey`
    """)
    spark.sql(f"""
        MERGE INTO {catalog}.{database}.DimCustomer_InferStage s
        USING (SELECT DISTINCT d.`CustomerKey` FROM {catalog}.{database}.DimCustomer d LEFT SEMI JOIN {catalog}.{database}.DimCustomer_InferStage x ON d.`CustomerKey` = x.`CustomerKey`) d
        ON s.`CustomerKey` = d.`CustomerKey`
        WHEN MATCHED THEN DELETE
    """)
    print('DimCustomer: inferred rows inserted, stage cleared')
except Exception as _e:
    _infer_errors.append('DimCustomer: ' + str(_e))
    print('ERROR DimCustomer: ' + str(_e))

# COMMAND ----------

# DimProduct: insert the staged members that are still missing, then clear the stage
try:
    spark.sql(f"""
        INSERT INTO {catalog}.{database}.DimProduct
          (`ProductKey`, `ProductKey_Id`, `ProductID`, Audit_CreatedDateTime, Audit_ModifiedDateTime, Audit_IsInferred, `Audit_IsUnknown`)
        SELECT s.`ProductKey`, MAX(s.`ProductKey_Id`), MAX(s.`ProductID`), current_timestamp(), current_timestamp(), true, MAX(s.`Audit_IsUnknown`)
        FROM {catalog}.{database}.DimProduct_InferStage s
        LEFT ANTI JOIN {catalog}.{database}.DimProduct d ON s.`ProductKey` = d.`ProductKey`
        GROUP BY s.`ProductKey`
    """)
    spark.sql(f"""
        MERGE INTO {catalog}.{database}.DimProduct_InferStage s
        USING (SELECT DISTINCT d.`ProductKey` FROM {catalog}.{database}.DimProduct d LEFT SEMI JOIN {catalog}.{database}.DimProduct_InferStage x ON d.`ProductKey` = x.`ProductKey`) d
        ON s.`ProductKey` = d.`ProductKey`
        WHEN MATCHED THEN DELETE
    """)
    print('DimProduct: inferred rows inserted, stage cleared')
except Exception as _e:
    _infer_errors.append('DimProduct: ' + str(_e))
    print('ERROR DimProduct: ' + str(_e))

# COMMAND ----------

if _infer_errors:
    raise RuntimeError('InferMissingRows failed: ' + chr(10).join(_infer_errors))
print("InferMissingRows complete.")