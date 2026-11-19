# Surrogate Keys and Foreign Keys: How the Hashing Works

This guide explains how BionToolkit computes **surrogate keys (SKs)** on dimensions,
**hashed foreign keys (FKs)** on facts, and their **integer (BIGINT) companion keys**. It
covers the exact SQL that gets generated and the rules that make an FK match the SK it
points at.

The short version: an SK is a hash of the dimension's business keys. A hashed FK is the
*same* hash, computed on the fact from columns that carry the same business-key values. If
both sides produce byte-identical text before hashing, the hashes match and the fact joins
to the dimension. No lookup or join happens at load time.

---

## 1. Concepts

| Term | Meaning |
|---|---|
| **Source key** | A column with `is_source_key: true`. The business/natural key. A table can have several (compound key). |
| **Surrogate key (SK)** | A `BINARY` column with `is_surrogate_key: true`, computed by hashing the table's source keys. At most one per table. |
| **Hashed FK** | A column with `foreign_table` set and **no** `expression`. Computed by hashing the referenced dimension's business-key values, supplied by the fact's source query. |
| **Expression FK** | A column with `foreign_table` set **and** an `expression`. Not hashed; the expression is used as-is. |
| **Integer key** | A `BIGINT` companion of an SK or hashed FK, named `<key column><int_key_identifier>` (default suffix `_Id`). |
| **Pre-image** | The concatenated string that is fed to the hash function. Everything in this document is about making pre-images match. |

---

## 2. The settings that control hashing

All in `processing` in `settings.yaml`:

| Setting | Default | Role |
|---|---|---|
| `surrogate_keys_enabled` | `false` | Generate SKs at all. |
| `passthrough_surrogate_keys` | `false` | Copy the SK from a source column instead of hashing. Incompatible with hashed FKs. |
| `hash_function` | `UNHEX(SHA2(<stringvalue>, 256))` | Binary hash template for SKs, FKs and `ChangeHash`. Stores the raw 32-byte digest. |
| `hash_multi_value_delimiter` | `\|` | Placed between key values in the pre-image. |
| `global_null_key_replacement_function` | `IFNULL(<value>, 'NULL')` | Applied to each value. `<value>` is already `CAST(... AS STRING)`. |
| `int_surrogate_keys_enabled` | `true` | Generate the BIGINT companions. |
| `int_key_hash_function` | `xxhash64(<stringvalue>)` | Integer-key hash template, applied to the **same pre-image**. |
| `int_key_identifier` | `_Id` | Suffix for integer key column names. Blank = no integer keys. |

> **Every one of these determines stored key values.** Changing any of them (or a source
> key column's data type) changes every SK, FK, integer key and `ChangeHash`. Because tables
> merge on the SK, an unreloaded table then gets duplicate rows. Reload dimensions and facts
> together.

`hash_int_conversion_function` is deprecated and ignored. The old
`conv(hex(<binary key>), 16, 10)` derivation overflowed on 32-byte digests. The integer key
is always `int_key_hash_function` over the pre-image, never derived from the binary hash.

---

## 3. Building the pre-image

Every notebook layout (per-table, consolidated, parameterized) and DLT pipelines build the
hash with the same shared code, so they all produce identical SQL.

For a list of columns `[A, B, C]`, the pre-image is:

```sql
CONCAT(
  IFNULL(CAST(A AS STRING), 'NULL'), '|',
  IFNULL(CAST(B AS STRING), 'NULL'), '|',
  IFNULL(CAST(C AS STRING), 'NULL')
)
```

Rules:

- **Order is the list order.** Nothing is sorted. For an SK, the list is the table's
  active `is_source_key` columns in the order they are declared in the YAML.
- **Cast to STRING happens inside the null replacement.** With ANSI mode on,
  `IFNULL(<int col>, 'NULL')` would try to cast `'NULL'` to an integer and fail with
  `CAST_INVALID_INPUT`. Casting first avoids that.
- **NULL becomes the literal text `NULL`** (with the default template). So a NULL key and the
  string `'NULL'` hash identically. This is a known property of the default; change
  `global_null_key_replacement_function` if it matters for your data.
- **Columns are backtick-quoted** when `add_grave_accent` is on.

The hash template's `<stringvalue>` placeholder is then replaced with this expression.

---

## 4. Surrogate keys (dimension side)

### When an SK is generated

An SK is computed when all of these hold:

- `surrogate_keys_enabled: true`
- `passthrough_surrogate_keys: false`
- the table has exactly one active `is_surrogate_key` column

You don't have to declare the SK yourself. When `surrogate_keys_enabled` is on and no column
is flagged `is_surrogate_key`, a `SurrogateKey BINARY` column is synthesized as the first
column. Its integer companion (`SurrogateKey_Id BIGINT`) is synthesized right after it. A
column you declare yourself is always kept as written.

### Generated SQL

Example: `DimProductLocation` with source keys `ProductCode STRING`, `LocationCode STRING`,
and an SK called `SurrogateKey`.

**Without an integer key**, the SK is one inline expression:

```sql
CAST(UNHEX(SHA2(CONCAT(
    IFNULL(CAST(ProductCode AS STRING), 'NULL'), '|',
    IFNULL(CAST(LocationCode AS STRING), 'NULL')
), 256)) AS BINARY)                                   AS SurrogateKey
```

**With an integer key**, the pre-image is computed once into a scratch column, both
hashes read it, and the scratch column is dropped before the MERGE:

```text
__bion_hash_preimage = CONCAT(IFNULL(CAST(ProductCode AS STRING),'NULL'), '|',
                              IFNULL(CAST(LocationCode AS STRING),'NULL'))
SurrogateKey         = CAST(UNHEX(SHA2(__bion_hash_preimage, 256)) AS BINARY)
SurrogateKey_Id      = CAST(xxhash64(__bion_hash_preimage) AS BIGINT)
-- then: drop __bion_hash_preimage
```

`__bion_hash_preimage` is a reserved name. It never reaches a target table, DDL or
`ChangeHash`. (In the parameterized layout it appears in the table config's
`column_drops` list.)

### Raw values, not expression output

For **table-based** sources the SK is hashed from the **raw** source-key columns, before any
per-column `expression` runs. (With a `table_query`, per-column expressions don't run at
all.) For **file-based** sources the expressions run in the source `SELECT`, so the SK
hashes the post-expression value. This distinction matters for FKs (section 5.4).

### SK as the merge identity

A table with a generated SK merges on it. Under SCD2 (`effective_dates_enabled`) the
identity is SK + `Audit_EffectiveDate`: every version of a business key shares the same SK.

---

## 5. Hashed foreign keys (fact side)

### 5.1 The design goal

A hashed FK must equal the dimension's SK for the same business key. To get there, the fact
reproduces the dimension's pre-image exactly, using values the fact's source query
provides. The FK column's own name never appears in the hashed text. That is what lets
**role-playing** FKs (`SK_OrderDate`, `SK_ShipDate`, both pointing at `DimDate`) each hash
to `DimDate`'s own SK.

### 5.2 The `foreign_*` source columns

For each hashed FK, the fact's `table_query` must supply **one column per source key of the
referenced dimension**, named:

```text
foreign_<FK column name>_<dimension source key column name>
```

Example. `DimProductLocation` has source keys `ProductCode`, `LocationCode`; `DimDate` has
source key `DateKey DATE`:

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
  - { name: OrderKey,           data_type: STRING, is_source_key: true }
  - { name: SK_ProductLocation, data_type: BINARY, foreign_table: DimProductLocation }
  - { name: SK_OrderDate,       data_type: BINARY, foreign_table: DimDate }
  - { name: SK_ShipDate,        data_type: BINARY, foreign_table: DimDate }
  - { name: Quantity,           data_type: INTEGER }
```

The `foreign_*` columns are scratch inputs. They are dropped right after hashing and never
reach the target table, its DDL, `ChangeHash` or data-quality expectations. (One exception:
when InferMissingRows is on, they are kept until the staging cell has used them, then
dropped. See section 7.)

### 5.3 Field order

**The order of the parts in an FK's pre-image is the declared order of the dimension's
active `is_source_key` columns.** The fact table has no say in it:

1. The toolkit takes the dimension's active `is_source_key` columns in the order they appear
   in the dimension's YAML.
2. For each one, it expects the matching `foreign_*` column on the fact.
3. It concatenates those values in that same order. Validation, the runtime column check
   and every generator use this one ordered list.

What this means in practice:

- The column order in the fact's `SELECT` doesn't matter. `foreign_*` columns are matched by
  name.
- **Reordering a dimension's source-key columns in YAML changes its SK and every FK that
  points at it.** Treat it like changing a hash setting: reload the dimension and its facts.
- Marking a key column `is_active: false` removes it from both the SK and the FK pre-images.

### 5.4 Type casting and expressions

Each `foreign_*` value is cast to the **declared data type of the matching dimension key
column**, then to STRING, then passed through the null replacement:

```sql
-- STRING-family key (STRING / CHAR / VARCHAR / TEXT): straight to plain STRING, no CHAR padding
IFNULL(CAST(foreign_SK_ProductLocation_ProductCode AS STRING), 'NULL')

-- Any other type, e.g. DateKey DATE: cast to the declared type first
IFNULL(CAST(CAST(foreign_SK_OrderDate_DateKey AS DATE) AS STRING), 'NULL')
```

The extra cast makes the fact render the value as text the same way the dimension did. If
the fact's source has `OrderDate` as a `TIMESTAMP` but `DimDate.DateKey` is declared `DATE`,
the cast turns `2026-11-17 00:00:00` into `2026-11-17`, matching the dimension.

> **Pitfall:** the fact side casts to the *declared* type, while the dimension hashes its
> raw source column. If the declaration doesn't match the dimension's actual source data
> (say the source is `TIMESTAMP` but the YAML says `DATE`), the two sides render different
> text and **no FK ever matches**. Declare dimension key types to match the source.

**Dimension key `expression`s:**

- **Table-based dimension (the only kind hashed FKs support):** the SK is hashed from the
  raw key, so the fact side applies no expression. Put the same raw value in the `foreign_*`
  column that the dimension's source delivers.
- **File-based dimension:** the SK hashes the post-expression value, so `ForeignKeyPart`
  carries the key's `expression` and the fact applies it before casting. In practice this
  path is unused, because file-based models can't have hashed FKs (section 8).

TIMESTAMP keys are rendered with the cluster's `spark.sql.session.timeZone`. Keep it the
same on every cluster that loads related tables.

### 5.5 The unknown member (all-NULL references)

For an FK with **more than one** part, the pre-image is wrapped so that a reference whose
`foreign_*` values are **all** NULL hashes to a single "unknown member" key, whatever the
key's arity:

```sql
CASE
  WHEN foreign_SK_ProductLocation_ProductCode IS NULL
   AND foreign_SK_ProductLocation_LocationCode IS NULL
  THEN IFNULL(CAST(NULL AS STRING), 'NULL')            -- pre-image text: NULL
  ELSE CONCAT(
         IFNULL(CAST(foreign_SK_ProductLocation_ProductCode AS STRING), 'NULL'), '|',
         IFNULL(CAST(foreign_SK_ProductLocation_LocationCode AS STRING), 'NULL'))
END
```

A single-part FK yields the text `NULL` naturally. The all-NULL test always looks at the
**raw** `foreign_*` columns, never at expression output.

Consequences:

- **A hashed FK is never NULL.** A missing reference becomes the unknown-member hash, so an
  expectation like `SK_X IS NOT NULL` never fires. To find dangling references, join the
  fact to the dimension on the dimension's SK.
- A *partially* NULL reference (e.g. `ProductCode='A'`, `LocationCode=NULL`) is not
  unknown: it hashes `A|NULL`.

> **Known limitation:** the dimension's own SK has no matching all-NULL rule. For a
> dimension with a **compound** key, a real source row whose keys are all NULL hashes
> `NULL|NULL`, not `NULL`, so its SK differs from the fact's unknown-member reference. For
> single-key dimensions both sides produce `NULL` and match. The unknown-member row that
> InferMissingRows inserts uses the fact-side hash, so facts match it either way.

### 5.6 Generated SQL for an FK

With an integer key, the generated code for `SK_ProductLocation` is:

```text
__bion_hash_preimage    = <CASE ... END pre-image from 5.5>
SK_ProductLocation      = CAST(UNHEX(SHA2(__bion_hash_preimage, 256)) AS BINARY)
SK_ProductLocation_Id   = CAST(xxhash64(__bion_hash_preimage) AS BIGINT)
-- drop foreign_SK_ProductLocation_ProductCode, foreign_SK_ProductLocation_LocationCode
-- drop __bion_hash_preimage (after all key expressions)
```

Because the integer key hashes the same pre-image, `SK_ProductLocation_Id` equals
`DimProductLocation.SurrogateKey_Id`. You can join on either the binary or the integer key.

---

## 6. Other FK variants

| Case | Behaviour |
|---|---|
| **Expression FK** (`foreign_table` + `expression`) | The expression is used verbatim. No `foreign_*` columns, no dimension needed, **no integer key**. Use it for lookups or constants. |
| **Unresolved FK** (`foreign_table` isn't an active, generated table in the model, e.g. inactive or `skip_on_build`) | Validation **warning**, not an error. The FK loads as `CAST(NULL AS BINARY)` and its integer key as `CAST(NULL AS BIGINT)`. No `foreign_*` columns are expected. Add the dimension and reload the fact to populate. |
| **Dimension exists but has no source keys** | Validation **error**: there's nothing to hash. |

---

## 7. InferMissingRows (early-arriving facts)

With `infer_missing_records_enabled` (or table-level `infer_missing_records`), each fact
notebook stages every hashed FK value that has no matching dimension row into
`<Dimension>_InferStage`. It stages the SK, the integer key and the business keys (the
`foreign_*` values cast to the dimension key types). After all table notebooks finish,
the shared `InferMissingRows` notebook inserts the placeholder rows into the dimension:

- Rows are inserted with `Audit_IsInferred = true`. When the real member arrives, the
  dimension MERGE (on the SK) overwrites the row and clears the flag.
- `Audit_IsUnknown = true` marks the unknown-member row (all keys NULL).
- Not-in-source deletes skip inferred and unknown rows.
- Expression FKs produce an SK-only row.
- Skipped (with a warning): dimensions outside the model, the built-in date table,
  passthrough SKs, dimensions without a generated SK, and DLT/streaming.

This only works because the FK hash equals the SK: the staged FK value *is* the SK of
the row that gets inserted.

---

## 8. Validation and runtime checks

`biontoolkit validate` (also run by `generate`/`deploy`) rejects:

- a hashed FK whose dimension has no source keys;
- a hashed FK when `passthrough_surrogate_keys` is on (dimension SKs aren't hashed, so
  nothing could match);
- a hashed FK in a model with a **file-based source** (file reads never run the
  `table_query`, so `foreign_*` columns can't exist);
- a hashed FK on a table without a `table_query`;
- an expected `foreign_*` name that collides with another FK's expected name or with a
  declared column.

It **warns** when a dimension isn't in the model (section 6), and when an expected
`foreign_*` name doesn't appear as a whole identifier in the `table_query` text (best
effort; SQL comments are ignored).

At **runtime**, right after the source is read, the generated code checks
(case-insensitively) that every expected `foreign_*` column exists. If any is missing, it
raises an error naming the missing columns and the naming rule. There is no fallback. In
the parameterized layout the list travels as `required_source_columns` in the table config.

**DLT / Lakeflow:** the `table_query` isn't executed. The pipeline reads
`<source_catalog>.<source_database>.<TableName>` by name, so that object must supply the
`foreign_*` columns itself. The CDC path (`create_auto_cdc_flow`) computes no hashes at all.

---

## 9. Checklist: why doesn't my FK match?

1. **Same model?** The dimension must be an active, generated table in the same model.
2. **Right column names?** `foreign_<FK column>_<dimension key column>`, one per dimension
   source key.
3. **Same values?** The `foreign_*` column must carry the raw value the dimension's source
   delivers, not the output of a dimension key `expression`.
4. **Declared types right?** The dimension's key `data_type` must match its real source
   data (`DATE` vs `TIMESTAMP` is the usual culprit).
5. **Key order changed?** Reordering or deactivating dimension source keys changes the SK.
   Reload both sides.
6. **Hash settings changed?** Any change to the section 2 settings requires a reload of
   dimensions and facts together.
7. **Time zone consistent?** For TIMESTAMP keys, every cluster needs the same
   `spark.sql.session.timeZone`.
8. **All-NULL compound key on the dimension?** See the caveat in section 5.5.

---

## 10. Further reading

The `biontoolkit` skill reference (`.claude/skills/biontoolkit/references/yaml-cli-reference.md`,
installed by `biontoolkit update-docs`) has the full settings tables. See its "Surrogate /
hash keys", "Foreign keys (hashed FK columns)" and "Upgrade notes" sections.
