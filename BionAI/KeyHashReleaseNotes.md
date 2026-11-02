# Key hashing: configuring primary and foreign keys (BionToolkit 0.5.60)

This document explains how to configure surrogate (primary) keys and foreign keys from
BionToolkit 0.5.60 onward, and how that differs from earlier releases. It is for engineers who
write or maintain BionToolkit model YAML.

**Releases compared**

| Release | What changed for keys |
|---|---|
| 0.5.57 and earlier | Original behavior. Described under each "Previous" heading below. |
| 0.5.58 | New hash format (32-byte digest, string-cast NULL handling, `xxhash64` integer keys). Foreign keys were still configured the old way. |
| 0.5.60 | Foreign keys are computed from `foreign_*` source columns. Tables merge on the surrogate key. Inferred dimension rows carry business keys and two flags. (There is no 0.5.59.) |

> **Breaking.** Every fact `table_query` with foreign keys must be rewritten. Stored key
> values change if you're upgrading from 0.5.57 or earlier. Reload dimensions and facts
> together. See [Upgrade checklist](#upgrade-checklist).

---

## 1. Summary of changes

| Topic | Previous | Now (0.5.60) |
|---|---|---|
| Binary key value | `SHA2(...)`: 64-byte hex text stored as BINARY | `UNHEX(SHA2(...))`: raw 32-byte digest |
| NULL handling in the hash | `IFNULL(col, 'NULL')`: fails under ANSI mode for non-string NULLs | `IFNULL(CAST(col AS STRING), 'NULL')`: every value is a string before hashing |
| Integer key | `xxhash64`, or `CONV(HEX(...))` if `hash_int_conversion_function` was set | Always `int_key_hash_function` (default `xxhash64`) over the same text as the binary key |
| FK input | The business key, aliased as the FK column itself (`s.CustomerKey AS SK_Customer`) | One `foreign_<FK>_<dimension key>` column per dimension source key |
| FK hashed text | The single value in the FK column, hashed as-is | Exactly the text the dimension hashes for its own surrogate key |
| Compound dimension keys | Not supported | Supported |
| Role-playing dimensions (two FKs to one dimension) | Only by convention, and inference broke | Supported |
| FK integer key | Hashed from the already-hashed binary FK, so it never matched the dimension | Hashed from the same text as the dimension's own integer key, so it matches |
| MERGE identity | Source keys or surrogate key, chosen by `merge_on_surrogate_key` | Always the surrogate key when the table has one |
| Inferred dimension rows | Surrogate key only, inserted into a column named after the fact's FK | Surrogate key, integer key and business keys, flagged `Audit_IsInferred` / `Audit_IsUnknown` |

---

## 2. Surrogate (primary) keys

### Configuration now

Model `settings.yaml`:

```yaml
settings:
  processing:
    surrogate_keys_enabled: true
    hash_function: "UNHEX(SHA2(<stringvalue>, 256))"    # default; may be omitted
    int_key_hash_function: "xxhash64(<stringvalue>)"    # default; may be omitted
    hash_multi_value_delimiter: "|"                     # default
    global_null_key_replacement_function: "IFNULL(<value>, 'NULL')"   # default
    int_surrogate_keys_enabled: true                    # default
    int_key_identifier: "_Id"                           # default; blank = no integer keys
```

Table YAML: mark the business key columns with `is_source_key: true`. Declaring the surrogate
key column is optional. If you leave it out, the engine adds a BINARY column named
`SurrogateKey`, plus its integer column when integer keys are on.

```yaml
table_name: DimCustomer
columns:
  - { name: SK_Customer,    data_type: BINARY, is_surrogate_key: true }
  - { name: SK_Customer_Id, data_type: BIGINT }               # optional; added automatically if omitted
  - { name: CustomerKey,    data_type: STRING, is_source_key: true }
  - { name: CustomerName,   data_type: STRING }
```

Compound keys work the same way: mark each column `is_source_key: true`. Declared order is
key order.

**Column position.** A generated `SurrogateKey` is the table's first column, and each
generated integer key sits right after its key: `SurrogateKey_Id` after `SurrogateKey`, and
`SK_Customer_Id` after the FK `SK_Customer`. Columns you declare yourself keep the position
you gave them. Previously the generated columns were added at the end of the table. Existing
tables keep their column order; the new order applies to new or reloaded tables.

### What gets hashed

1. Each source-key column is cast to `STRING`.
2. NULL is replaced, giving `IFNULL(CAST(col AS STRING), 'NULL')`.
3. The values are joined with `|`, in declared order. This is the *pre-image*.
4. The pre-image is hashed:
   - **Binary key:** `UNHEX(SHA2(pre-image, 256))`, a 32-byte value.
   - **Integer key:** `xxhash64(pre-image)`, a BIGINT.

The pre-image is computed once, in a scratch column `__bion_hash_preimage` that is dropped
before the write. Don't name a column `__bion_hash_preimage`.

| Source key values | Pre-image |
|---|---|
| `CustomerKey = 'C001'` | `C001` |
| `ProductCode = 'P1'`, `LocationCode = 'L9'` | `P1\|L9` |
| `ProductCode = 'P1'`, `LocationCode = NULL` | `P1\|NULL` |
| `DateKey = DATE '2026-10-30'` | `2026-10-30` |

The binary key, any foreign key that references it and `ChangeHash` all use
`hash_function`. The integer keys use `int_key_hash_function`.

**Hashed values depend on the string form, so declare types carefully:**
- `DATE` and `TIMESTAMP` render differently (`2026-10-30` vs `2026-10-30 00:00:00`).
- Decimal scale is part of the text (`DECIMAL(18,2)` gives `1.50`; `DECIMAL(18,4)` gives
  `1.5000`).
- `TIMESTAMP` keys are converted using the cluster's `spark.sql.session.timeZone`. Keep it the
  same on every cluster that loads a table.

### How tables merge now

Every table with a generated surrogate key merges on it:

```sql
MERGE INTO DimCustomer AS target USING _source_data AS source
ON target.SK_Customer = source.SK_Customer
```

- **SCD2 tables** (`effective_dates_enabled: true`) merge on the surrogate key plus
  `Audit_EffectiveDate`. All versions of a business key share one surrogate key.
- **Tables without a surrogate key** merge on their `is_source_key` columns.
- **`passthrough_surrogate_keys`:** these tables merge on the supplied key, except under SCD2,
  where each version carries its own key and the source keys stay the identity.

`merge_on_surrogate_key` is **deprecated and ignored**. Generation prints a warning if it's set.

### Previous method (0.5.57 and earlier)

- **Binary key:** the default `hash_function` was `SHA2(<stringvalue>, 256)`. That stored the
  64-character hex string as BINARY.
- **NULLs:** each column was wrapped as `IFNULL(col, 'NULL')` without a cast. On ANSI-mode
  clusters (serverless, SQL warehouses, Spark 4), a NULL in an `INT`, `DATE`, `DECIMAL` or
  `TIMESTAMP` key failed with `CAST_INVALID_INPUT`.
- **Integer key:** `xxhash64` over the concatenation, unless `hash_int_conversion_function`
  was set. That used `CONV(HEX(<binary key>), 16, 10)`, which overflows on a 32-byte digest.
  `hash_int_conversion_function` is now ignored, and generation prints a warning if it's set.
- **MERGE identity:** chosen by `merge_on_surrogate_key`, either source keys or surrogate key.

**To keep 0.5.57 binary values,** set `hash_function: "SHA2(<stringvalue>, 256)"`.
- Hashes of non-NULL values then stay the same.
- Foreign-key integer keys still change (they were wrong before).
- `ChangeHash` still changes for tables whose old hash included integer key columns or
  lowercase `audit_*` columns.

---

## 3. Foreign keys

### Configuration now

A **hashed foreign key** is a fact column with `foreign_table` set and **no** `expression`.
Configure it in three places.

**1. The dimension** (same model) defines the key: its `is_source_key` columns, in declared
order, with their declared data types.

```yaml
table_name: DimProductLocation
columns:
  - { name: SK_ProductLocation, data_type: BINARY, is_surrogate_key: true }
  - { name: ProductCode,        data_type: STRING, is_source_key: true }
  - { name: LocationCode,       data_type: STRING, is_source_key: true }
```

**2. The fact column** names the dimension. Nothing else is needed: no key columns, no
lookup.

```yaml
  - { name: SK_ProductLocation, data_type: BINARY, foreign_table: DimProductLocation }
```

**3. The fact's `table_query`** supplies one column per dimension source key, named:

```
foreign_<FK column name>_<dimension source key column name>
```

```yaml
table_name: FactSales
settings:
  table_query: |
    SELECT
        s.OrderKey,
        s.ProductCode  AS foreign_SK_ProductLocation_ProductCode,
        s.LocationCode AS foreign_SK_ProductLocation_LocationCode,
        s.OrderDate    AS foreign_SK_OrderDate_DateKey,
        s.ShipDate     AS foreign_SK_ShipDate_DateKey,
        s.Quantity
    FROM silver.SalesOrder s
columns:
  - { name: SK_Sales,           data_type: BINARY, is_surrogate_key: true }
  - { name: OrderKey,           data_type: STRING, is_source_key: true }
  - { name: SK_ProductLocation, data_type: BINARY, foreign_table: DimProductLocation }
  - { name: SK_OrderDate,       data_type: BINARY, foreign_table: DimDate }
  - { name: SK_ShipDate,        data_type: BINARY, foreign_table: DimDate }
  - { name: Quantity,           data_type: INTEGER }
```

`DimDate` has one source key, `DateKey`. `SK_OrderDate` and `SK_ShipDate` both reference it:
the FK name appears only in the `foreign_*` column name, never in the hashed text. Both
therefore hash to `DimDate`'s own `SK_Date` values. That is how role-playing dimensions work.

### What the engine does with them

1. **Checks the columns exist.** Right after reading the source, the generated code verifies
   every expected `foreign_*` column is present (case-insensitive). If any are missing, it
   stops with an error listing the missing names. There is no fallback to the old behavior.
2. **Builds the dimension's pre-image.** Each `foreign_*` value is cast to the **dimension key
   column's declared type**, then to `STRING`, with the same null replacement and delimiter
   the dimension uses. `CHAR(n)`, `VARCHAR(n)` and `STRING` keys are cast straight to `STRING`.
3. **Hashes it** into the FK column. The FK's integer column (for example `SK_OrderDate_Id`)
   hashes the same pre-image, so it equals the dimension's integer key.
4. **Drops the `foreign_*` columns.** They never reach the target table, its DDL or
   `ChangeHash`.

**Unknown member:** if **all** of an FK's `foreign_*` values are NULL, the pre-image is a
single `NULL`, not `NULL|NULL`. Every fully-NULL reference therefore shares one key, the
unknown-member key, however many key columns the dimension has. A partially NULL reference
hashes normally, for example `P1|NULL`.

### Rules and validation

`validate`, `generate` and `deploy` **reject** a hashed FK when:
- `foreign_table` isn't an active, generated table of the same model (an inactive or
  `skip_on_build` dimension doesn't count);
- the dimension has no `is_source_key` columns, or uses `passthrough_surrogate_keys`;
- the fact has no `table_query`;
- the model's source is **file-based**, because file reads never run the `table_query`;
- an expected `foreign_*` name collides with another expected name or a declared column.

They **warn** when an expected `foreign_*` name doesn't appear as a whole identifier in the
`table_query` text. Names inside SQL comments don't count.

**To skip hashing** (for a constant or a lookup, say), give the FK column an `expression`. It
then needs no dimension, no `foreign_*` columns, and has no integer key.

**Dimension key `expression`:** a table-based dimension hashes its key *before* any per-column
`expression` runs. So each `foreign_*` column must carry the same raw value the dimension's
source delivers, not the transformed value.

**Declare dimension key types to match the source data.** The fact side casts to the declared
type. A dimension declared `DATE` whose source actually holds `TIMESTAMP` values produces keys
that never match.

**Supported layouts:** per-table, consolidated and parameterized notebooks with a table-based
source.
- **DLT / Lakeflow:** the `table_query` is not executed, so the upstream object
  `<source_catalog>.<source_database>.<TableName>` must already contain the `foreign_*`
  columns.
- **DLT CDC path:** `create_auto_cdc_flow` computes no FK hashes.

**Data-quality expectations** can't reference `foreign_*` columns, because they're dropped
first. A hashed FK is never NULL, so `SK_X IS NOT NULL` catches nothing. To find unmatched
references, join the fact to the dimension on the surrogate key.

### Previous method (0.5.58 and earlier)

The FK column was both input and output. The query delivered the dimension's business key
*under the FK column's own name*, and the generator hashed that one value in place:

```yaml
# Previous (no longer works)
table_query: |
  SELECT s.OrderKey,
         s.CustomerKey AS SK_Customer,   -- business key aliased as the FK column
         s.OrderDate   AS SK_OrderDate
  FROM silver.SalesOrder s
columns:
  - { name: SK_Customer,  data_type: BINARY, foreign_table: DimCustomer }
  - { name: SK_OrderDate, data_type: BINARY, foreign_table: DimDate }
```

```yaml
# Now
table_query: |
  SELECT s.OrderKey,
         s.CustomerKey AS foreign_SK_Customer_CustomerKey,
         s.OrderDate   AS foreign_SK_OrderDate_DateKey
  FROM silver.SalesOrder s
columns:
  - { name: SK_Customer,  data_type: BINARY, foreign_table: DimCustomer }
  - { name: SK_OrderDate, data_type: BINARY, foreign_table: DimDate }
```

The fact column definitions don't change. Only the query aliases do.

Limitations of the previous method:
- **Single-column keys only.** A compound dimension key couldn't be referenced.
- **No type agreement with the dimension.** The value was hashed in whatever type the query
  delivered. A value that differed in type or format from the dimension's key silently
  produced a non-matching FK.
- **Wrong FK integer key.** It was hashed from the binary FK rather than the business key, so
  it never equalled the dimension's integer key.
- **Inference failed for role-playing FKs.** InferMissingRows inserted into a dimension column
  named after the *fact's* FK (`DimDate.SK_OrderDate`), which fails when the names differ.

**Do stored FK values change?** Yes, when you're on the default `hash_function` and coming from
0.5.57 or earlier. Otherwise they stay the same only if the old query delivered the dimension's
single business key, in the same type and format as the dimension's declared key column.

---

## 4. Inferred dimension rows

This only applies with `infer_missing_records_enabled: true`, or a table-level
`infer_missing_records`. The table setting now overrides the model setting in both directions.

**How it works now:**
1. **Each fact notebook stages missing keys.** Before dropping the `foreign_*` columns, it
   writes the keys missing from each dimension to a staging table, `<Dimension>_InferStage`
   (created by `UpdateSchema`). It stages the surrogate key, the integer key and the business
   keys, cast to the dimension's types.
2. **The shared `InferMissingRows` notebook runs last.** `RunAllNotebooks` runs it after every
   table notebook has finished, including when some failed. It inserts the still-missing
   members, then clears the staging rows the dimension now holds.
3. **Inferred rows are flagged.** Dimensions that receive inferred rows get two `BOOLEAN`
   columns:

   | Column | Meaning |
   |---|---|
   | `Audit_IsInferred` | `true` when InferMissingRows created the row. It's set back to `false` when real source data arrives for that key. |
   | `Audit_IsUnknown` | `true` on the unknown-member row, where all business keys are NULL. |

   Both names are reserved on those dimensions. Neither is part of `ChangeHash`.
4. **The real member replaces the inferred row in place.** Because the inferred row carries
   the real business key and surrogate key, the dimension's normal MERGE overwrites it. Under
   SCD2 it's updated in place (keeping its effective date), not versioned.
5. **Not-in-source deletes skip flagged rows.** Hard deletes, soft deletes and SCD2 expiry all
   leave rows flagged inferred or unknown alone.

**Previously:** each fact notebook inserted directly into the dimension, in parallel with the
dimension loads. It wrote only the key, into a column named after the fact's FK. The inferred
row had NULL business keys, so the dimension's next load inserted the real member as a
**second row with the same surrogate key**.

---

## 5. ChangeHash (related)

`ChangeHash` uses the same string-cast pre-image and `hash_function`. It now covers source
business columns only. It excludes:
- source keys, the surrogate key and FK columns;
- the high-watermark column;
- generated integer keys;
- all `Audit_*` columns, in any case;
- `ChangeHash` itself.

Columns with an `expression` are still included.

---

## Upgrade checklist

1. **Install 0.5.60.** It requires bion-etl-kernel 0.1.3.
2. **Remove `hash_int_conversion_function`** from `settings.yaml` if it's set.
3. **Remove `merge_on_surrogate_key`** if it's set (it's ignored).
4. **Rewrite every fact `table_query` with hashed FKs.** Replace `s.<business key> AS <FK>`
   with one `foreign_<FK>_<dimension key>` column per dimension source key, in the dimension's
   key order.
5. **Check dimension key types** match the real source data, especially `DATE` vs `TIMESTAMP`
   and decimal scale.
6. **Run `biontoolkit validate`** and fix every error and warning about missing dimensions,
   missing `foreign_*` names and file-based sources.
7. **Regenerate and redeploy all notebooks together,** including the shared notebooks
   (`RunAllNotebooks`, `UpdateSchema`, `InferMissingRows`) and, in the parameterized layout,
   the `table_configs` JSON. Run `UpdateSchema` before the first load.
8. **Reload dimensions and facts together** if any stored key changed: you're coming from
   0.5.57 or earlier with the default `hash_function`, or your old FK query didn't match the
   dimension key exactly. Tables now merge on the surrogate key, so a table whose stored keys
   don't match the current hash gets **duplicate rows** instead of updates.

**From now on, reload tables after changing** `hash_function`,
`global_null_key_replacement_function`, `int_key_hash_function`, a source key column's data
type, or the cluster's session time zone (for `TIMESTAMP` keys). Any of these changes the
stored keys.

---

## Known limitations

- **DLT / Lakeflow:** hashed FKs need the `foreign_*` columns in the upstream object. Inferred
  rows aren't supported (generation prints a warning).
- **`truncate_and_load` dimensions** lose inferred rows on every run (a warning is printed).
- **Rows inferred before 0.5.60** have `Audit_IsInferred` NULL, which counts as `false`, so
  they're not protected from not-in-source deletes. `Audit_IsUnknown` is back-filled by
  `UpdateSchema`; `Audit_IsInferred` can't be, because there's no record of which old rows
  were inferred.
- **Turning inference off later** doesn't remove an existing `InferMissingRows` notebook or
  `<Dimension>_InferStage` tables. Drop them manually if you want.
