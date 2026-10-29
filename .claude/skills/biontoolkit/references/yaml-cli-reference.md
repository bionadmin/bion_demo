# biontoolkit — Full CLI and YAML Reference

Loaded on demand from the `biontoolkit` skill. See `../SKILL.md` for installation and
the top-level overview.

## CLI Commands

### `generate` — Generate notebooks from a model

```bash
biontoolkit generate --model <path-to-model-dir-or-yaml>
biontoolkit generate --model models/silver/CustomerConformed --dry-run
```

- `--model` (required) — path to a model directory or single-file YAML
- `--dry-run` — print generated notebooks to stdout without writing files
- `--bundle` — also generate a Databricks Asset Bundle for this run (equivalent to setting `deployment.bundle_enabled: true` just for this invocation)
- `--classic` — skip Databricks Asset Bundle generation for this run, even if `deployment.bundle_enabled: true` (mutually exclusive with `--bundle`)

---

### `validate` — Validate model YAML

```bash
biontoolkit validate --model models/silver/CustomerConformed
```

Reports structural errors, missing required fields, and invalid column definitions.

Model-level required fields are validated before per-table checks. Missing `dest_catalog` or `target_database` produces an explicit error rather than silently generating invalid Unity Catalog 3-part names.

---

### `init-model` — Scaffold a new model

```bash
biontoolkit init-model --name RawIngestion --zone bronze
biontoolkit init-model --name CustomerConformed --zone silver
biontoolkit init-model --name SalesDW --zone gold
```

- `--name` (required) — model name
- `--zone` (required) — any string; common values are `silver`, `gold`, `bronze`

Creates a new directory with a starter `settings.yaml` and an example table file.

---

### `deploy` — Deploy generated notebooks

```bash
biontoolkit deploy --model models/silver/CustomerConformed
biontoolkit deploy --model models/silver/CustomerConformed --dry-run
biontoolkit deploy --model models/silver/CustomerConformed --bundle
```

- `--target local|api` — `local` (default) writes to the local repo folders; `api` uploads via REST API
- `--bundle` — also generate a Databricks Asset Bundle (`--target local` only)
- `--classic` — skip bundle generation even if `deployment.bundle_enabled: true` (mutually exclusive with `--bundle`)

Runs the full generate → deploy pipeline in a single step. Deploys to local path
(defined in `settings.yaml`) or to Databricks via REST API.

The `deploy` command applies the same `artifact_layout` routing as `generate`:
- `per_table` (default) — one notebook per table
- `consolidated` — all tables in a single notebook
- `parameterized` — one `{ModelName}_ProcessTable.py` driven by JSON widget config;
  `RunAllNotebooks` is generated with per-table config embedded
- `dlt` (`artifact_type: dlt` or `streaming_enabled: true`) — Delta Live Tables pipeline

By default `deploy --target local` writes the generated notebooks/pipeline files straight
into the local repo folders (classic deployment, no bundle). Pass `--bundle` (or set
`deployment.bundle_enabled: true`) to also produce a Databricks Asset Bundle
(`databricks.yml` plus a `resources/*.yml` job or pipeline definition) — see
[Databricks Asset Bundle (DAB) output](#databricks-asset-bundle-dab-output) below.
`--classic` forces bundles off for a run. `--target api` does not produce bundle files in this
version.

PAT token must be in the `DATABRICKS_TOKEN` environment variable — never in YAML.

---

### `workspace` — Manage workspace configuration

```bash
biontoolkit workspace create --name MyWorkspace --catalog dev_catalog --cloud azure
biontoolkit workspace list
biontoolkit workspace show
```

Reads/writes `workspace.yaml` in the current (or nearest parent) directory.

---

### `import` — Import a schema from an external source

```bash
# From Parquet files (uses pyarrow to read schema)
biontoolkit import parquet --source path/to/parquet-dir --name MyModel

# From a CSV file (uses header row for column names)
biontoolkit import csv --source path/to/file.csv --name MyModel

# From SQL Server (uses pyodbc + INFORMATION_SCHEMA)
biontoolkit import sql --connection-string "DRIVER=...;SERVER=...;DATABASE=..." --table dbo.Customer --name MyModel

# From a Databricks query (uses Statement Execution API 2.0)
biontoolkit import databricks-query --query "SELECT * FROM dev_catalog.silver.Customer LIMIT 0" --name MyModel
```

All import commands write a `settings.yaml` + one table YAML file per discovered table.
The Databricks import reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from environment variables.

---

### `diagram` — Generate a Draw.IO ER diagram

```bash
biontoolkit diagram --model models/silver/CustomerConformed
biontoolkit diagram --model models/silver/CustomerConformed --output ./diagrams
biontoolkit diagram --model models/silver/CustomerConformed --open
biontoolkit diagram --model models/silver/CustomerConformed --open --drawio-path "C:/Program Files/draw.io/draw.io.exe"
```

- `--output` — directory for the `.drawio` file (defaults to model directory)
- `--open` — launch Draw.IO desktop app after generating
- `--drawio-path` — path to Draw.IO executable (overrides workspace global setting)

---

### `update-docs` — Install consumer documentation

```bash
biontoolkit update-docs
biontoolkit update-docs --output /path/to/project
```

Copies bundled documentation and the `biontoolkit` Agent Skill into the target repository:

- `.github/copilot-instructions.md` — read automatically by GitHub Copilot
- `.github/developer-instructions.md` — human operator guide with prompting tips and workflows
- `.github/jdbc-driver-setup.md` — JDBC driver installation guide for SQL database sources
- `CLAUDE.md` (repo root) — read automatically by Claude Code
- `.claude/skills/biontoolkit/` (`SKILL.md` + this reference file) — the full CLI/YAML
  reference, loaded on demand by Claude Code and by GitHub Copilot in VS Code (both read
  `.claude/skills/`)

- `--output` (optional) — path to the target repository root; defaults to the current directory

Run this after upgrading biontoolkit to keep all documentation files and the skill reference current.

---

## Environment Variables

| Variable | Used by | Notes |
|----------|---------|-------|
| `DATABRICKS_HOST` | `deploy`, `import databricks-query` | Workspace URL, e.g. `https://adb-123.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | `deploy`, `import databricks-query` | PAT — never store in YAML |
| `<ENV:VARNAME>` in YAML paths | `deploy` | Substituted at deploy time from the environment |

Example path token usage in `settings.yaml`:
```yaml
deploy:
  local_process_path: "<ENV:REPO_ROOT>/notebooks/silver/tables"
  local_dependency_path: "<ENV:REPO_ROOT>/notebooks/silver/common"
```

> **Note:** `<ENV:...>` tokens only affect **where files are written** on the developer's machine. They are never baked into the generated notebooks — the generator strips the token prefix when computing the relative path offset between `local_process_path` and `local_dependency_path`.

---

## Model Structure

### Multi-file layout (recommended)

```
models/
  silver/
    CustomerConformed/
      settings.yaml        # model-level settings (source, destination, processing, deployment)
      tables/
        Customer.yaml      # one file per table
        Address.yaml
  gold/
    SalesDW/
      settings.yaml
      tables/
        DimCustomer.yaml
        FactSales.yaml
```

### `settings.yaml` — Silver model example

```yaml
model_name: CustomerConformed
zone: silver

settings:
  source:
    cloud_provider: azure          # azure | aws
    source_type: file              # file | table
    use_unity_catalog_volumes: true
    volume_name: bronze-raw
    source_catalog: dev_catalog
    source_database: bronze
    file_type: parquet
    file_extension: .parquet
    file_pattern: "Customer_*"
    per_table_subfolder: true
    use_auto_loader: true
    use_iso_dated_folders: true
    iso_date_folder_format: yyyyMMddHHmmss
    iso_folder_name_as_effective_date: true

  destination:
    zone: silver
    target_database: silver
    dest_catalog: dev_catalog
    managed_tables: false
    dest_cloud_provider: azure
    dest_storage_account: "<ENV:SILVER_STORAGE_ACCOUNT>"
    dest_container: silver
    dest_folder_path: conformed

  processing:
    effective_dates_enabled: true
    effective_dates_round_to_midnight: true
    change_hash_enabled: true
    table_statistics_enabled: true
    expectations_enabled: true
    include_create_statements: true
    optimize_enabled: true
    vacuum_enabled: true
    vacuum_retention_hours: 168
    ansi_mode: true
    add_grave_accent: true

  deployment:
    artifact_layout: per_table       # per_table | parameterized | consolidated
    local_process_path:    "<ENV:REPO_ROOT>/notebooks/silver/tables"   # local write path only
    local_dependency_path: "<ENV:REPO_ROOT>/notebooks/silver/common"  # must share a common prefix with local_process_path
```

---

## Upgrade notes

Read this when upgrading an existing model, or when a first run after an upgrade behaves
differently from before. Regenerate and redeploy all notebooks (and `table_configs` JSON in
the parameterized layout) after upgrading; old and new notebooks must not be mixed.

### Hash keys, integer keys and ChangeHash (0.5.58) — BREAKING

**Stored values change.** After upgrading, regenerate and redeploy, then reload (or rehash)
dimensions and facts **together**. Facts loaded before the upgrade do not join to dimensions
loaded after it.

- **Binary SK / FK / `ChangeHash` values:** the default `hash_function` is now
  `UNHEX(SHA2(<stringvalue>, 256))`, which stores the raw 32-byte SHA-256 digest instead of
  the 64-byte hex text. To keep the old binary values, pin
  `hash_function: "SHA2(<stringvalue>, 256)"` in `settings.yaml`.
- **`ChangeHash` scope:** it now covers real source business columns only. Integer key
  columns the engine generates, the `ChangeHash` column and `audit_*` columns in any case
  are no longer hashed (see [Change hash](#change-hash)). Every stored `ChangeHash` changes
  with the new default; even with a pinned `hash_function`, it changes for tables that had
  integer key columns or lowercase `audit_*` columns. On the first run a Type 1 table rewrites
  each matched row once, and an SCD2 table adds one version for each current key in the source,
  unless the tables are reloaded.
- **FK integer keys:** the integer key of a foreign key column now hashes the original FK
  value, so it equals the referenced dimension's integer SK. Before, it was computed from the
  already-hashed binary FK and never matched.
- **`hash_int_conversion_function` is deprecated and ignored.** If your model sets it,
  generation prints a warning and the integer key is `int_key_hash_function` (default
  `xxhash64(<stringvalue>)`), so those integer keys change. Remove the setting from
  `settings.yaml`. A model that set it together with `int_surrogate_keys_enabled: false` no
  longer gets integer key columns.
- **Unchanged:** default `xxhash64` integer SKs (they hash the key pre-image, not the binary
  key).

**Other changes**

- Each hashed column is now cast to STRING inside the null replacement, so the default renders
  `IFNULL(CAST(col AS STRING), 'NULL')`. This fixes `CAST_INVALID_INPUT` with ANSI mode on when
  a non-string key column is NULL. Hashes of non-NULL values do not change. A custom
  `global_null_key_replacement_function` now receives `CAST(col AS STRING)` as `<value>`, not
  the typed column.
- When an integer key is generated, the hash pre-image is computed once into a scratch column,
  `__bion_hash_preimage`, which is dropped before the data reaches the target table. Do not
  name a column `__bion_hash_preimage`.
- Parameterized layout: `table_configs` JSON has a new `column_drops` key. Deploy the
  `ProcessTable` notebook together with its regenerated `table_configs`.

### ISO-folder HWM and SCD2 / ChangeHash fixes

**What the first run after upgrading does**

- **Type 1 tables with `change_hash_enabled` (`per_table` and `consolidated` layouts):** every
  existing row whose key is in the source is rewritten once. The target `ChangeHash` was
  never stored before (always NULL), so matched rows were never updated; NULL now counts as
  changed. This also applies any updates that were missed before. Later runs update only
  changed rows. Parameterized tables already stored the hash; only rows with a NULL hash are
  rewritten.
- **SCD2 tables with `change_hash_enabled`:** each current row with a NULL `ChangeHash` whose
  key is in the source gets exactly one extra version (expired and re-inserted) on the first
  run, even if its data is unchanged. This affects every such row in `per_table` and
  `consolidated` tables. There is no backfill step: adopting a hash without proving the data
  equal could silently lose a real change. The result is extra history rows, not data loss.
- **SCD2 merge is now two statements.** (1) A MERGE that expires changed current rows and
  inserts new versions; unchanged keys are not touched. (2) Only with
  `not_in_source_deletes_enabled`: a second MERGE over the full source that deletes or
  soft-deletes CURRENT rows whose key is absent. Expired history is never deleted. A soft
  delete now also sets `Audit_ExpiredDate`. The two statements are not one transaction,
  but each is idempotent and the watermark is committed only after both, so a failed run
  re-runs cleanly.
- **`iso_folder_name_as_effective_date` now works.** It used to be ignored, so existing rows
  were dated by processing time. Models that already set it now date versions by folder name.
  On a populated SCD2 table the first folder is usually *older* than the newest current
  `Audit_EffectiveDate`, and the stale-folder guard fails the run before anything is written.
  Remedies: (a) reload the table (empty or drop the target; in folder-HWM mode also delete
  BOTH the table's `__iso_folder__` and `__file_path__` rows in `HighWatermark`, because a
  remaining legacy `__file_path__` row would set the restart point and skip every older
  folder), then run the driver so every folder is loaded oldest first; or (b) set the flag
  to `false` to keep processing-time dates.
  Never reset the `__iso_folder__` row to an older folder to work around the guard: that can
  only reprocess the newest loaded folder.
- **Folder names must fully match `iso_date_folder_format`.** Previously any name starting
  with 14 digits matched, whatever the format. Now `yyyyMMddHHmm` (12 digits) works, and names
  such as `20261016093000_old`, or names with a different digit count, are ignored.
- **Auto catch-up (ISO folders + `use_high_watermark` + `high_watermark_file_level`):** the
  table used to read only the newest folder. It now processes every folder newer than its
  watermark, oldest first, one child run per folder. The existing `__file_path__` watermark is
  read (never changed) to find the last loaded folder, so only folders that arrived after it
  are processed. There is no opt-in flag. Limit a large backlog with `max_folders_per_run`.
  The `consolidated` layout is not supported and keeps newest-folder-only behaviour.
- **Zero-row snapshot folder (folder-HWM mode + `not_in_source_deletes_enabled`):** a folder
  that has files but zero rows deletes (or soft-deletes) every current row, then the watermark
  advances. A folder with no matching files is skipped and never deletes.
- **Non-SCD2 soft delete now works** (`not_in_source_deletes_enabled` + `hard_deletes: false`
  + `effective_dates_enabled: false`). It used to generate a MERGE that failed on every run.
  The table now gets an `Audit_IsDeleted BOOLEAN` column (see `hard_deletes`); on an existing
  table it is added by the framework-column back-fill below. Existing rows hold NULL, which
  means not deleted. SCD2 soft delete and hard delete are unchanged.
- **Missing framework columns are added automatically.** `per_table` and `consolidated`
  notebooks now run an idempotent back-fill cell right after `CREATE TABLE IF NOT EXISTS`
  (same `include_create_statements` gate): any missing `ChangeHash`, `Audit_EffectiveDate`,
  `Audit_ExpiredDate`, `Audit_RowIsCurrent`, `Audit_IsDeleted`, `Audit_CreatedDateTime` or
  `Audit_ModifiedDateTime` is added with `ALTER TABLE ... ADD COLUMNS`. The parameterized
  layout adds them through its pre-MERGE schema evolution. Running `UpdateSchema` first is
  optional (it is a standalone notebook that nothing runs automatically). Added columns are
  NULL on existing rows. A NULL `ChangeHash` (one re-hash, or one extra SCD2 version) and a
  NULL `Audit_IsDeleted` (not deleted) are safe; NULL SCD2 columns are initialised
  automatically (next bullet).
- **Legacy SCD2 rows are initialised automatically.** Rows with NULL `Audit_RowIsCurrent`
  (SCD2 columns added by `ALTER` to a populated table, Gold `merge_on_surrogate_key: true`
  tables written by the old SK merge, or Unknown rows inserted by InferMissingRows) get
  `Audit_RowIsCurrent = true`, `Audit_ExpiredDate = 9999-12-31` and `Audit_EffectiveDate =
  1900-01-01` ("valid since the start", so it never trips the stale-folder guard); only NULL
  dates are filled (`COALESCE`), so partially populated rows keep their real values. Every
  SCD2 notebook (`per_table`, `consolidated`, parameterized) does this before the SCD2 merge,
  and before the stale-folder guard, behind a probe (`SELECT 1 ... WHERE Audit_RowIsCurrent
  IS NULL LIMIT 1`), so steady-state runs rewrite nothing. Only keys that have no current row
  are initialised, so a key never ends up with two current rows. It does not run for Type 1,
  `truncate_and_load` or DLT. No manual `UPDATE` is needed. Limit: with `change_hash_enabled`
  the legacy rows have a NULL `ChangeHash`, so each key in the source gets one extra version on
  the first run, changed or not (extra history, not data loss). InferMissingRows now also
  inserts Unknown rows of an SCD2 dimension as current (`Audit_RowIsCurrent = true`,
  effective `1900-01-01`, expired `9999-12-31`), so a later real row supersedes them.
  Cost: the probe runs on every run of every SCD2 table. Delta collects column statistics
  (including null counts) only for the first 32 columns by default, and the framework columns
  come last, so on a wide table the probe may scan `Audit_RowIsCurrent` instead of skipping
  files. If that matters, add the column to the table's statistics columns, e.g.
  `ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '<existing filter
  columns>,Audit_RowIsCurrent')` (the list replaces the first-32 default, and it applies to
  newly written files). A table damaged by the old duplicate-row bug (a NULL row beside a
  current row for the same key) keeps its NULL rows, so it pays one no-op MERGE per run until
  it is reloaded.
- **Key-only SCD2 tables** (no non-key business columns) no longer re-version every key on
  every run: existing current keys are untouched, new or reappearing keys get a version, and
  not-in-source deletes still apply. Key-only non-SCD2 tables with the soft-delete flag
  rewrite a matched row only to un-delete it (nothing else can change).
- **The source is cached.** Every MERGE notebook except `truncate_and_load` (and streaming
  sources) persists and counts the final source once, keeps it through the MERGE, HWM commit,
  data quality audit write and InferMissingRows cells, then releases it in a dedicated cell;
  a failure in any of those cells also releases it. Expect one extra materialisation and
  cluster memory/disk for the source while the table runs. Caching is best-effort: where
  `persist()` is not supported (e.g. serverless compute) the notebook prints one line
  (`Source cache skipped (...); running uncached.`) and continues uncached. A source read
  error during the materialising count releases the cache and is raised as usual.
- **The `consolidated` layout now runs the stale-folder guard** for SCD2 +
  `iso_folder_name_as_effective_date` (newest-folder message); it had no guard before.
- **Type 4 history now captures changes from or to NULL** (the comparison is null-safe).
- **`loop_until_no_files_left` tables now actually loop.** Before, RunAllNotebooks ran them once
  and the parameterized layout ignored the setting.
  - Stopping: the loop stops on `no_files` or `files=0`, up to `max_loop_iterations`. See the
    `loop_until_no_files_left` row for the per-layout mechanics.
  - Expect longer runs where files keep arriving or a post-run step archives files.
  - With file-level HWM, the final iteration (no new files) no longer truncates a
    `truncate_and_load` table.
  - Without file-level HWM, a listing error other than a missing folder (for example a missing
    container or external location) fails the run instead of being treated as empty.

**New settings:** `source.max_folders_per_run` (default `0` = unlimited),
`source.iso_folder_initial_load` (`all` | `latest`; applies only to a table with no watermark
yet). `processing.notebook_run_timeout: 0` now means no timeout for folder child runs too.

**New validation errors (`biontoolkit validate`; also enforced at generation)**

- `effective_dates_enabled` + `merge_on_surrogate_key: true` with `passthrough_surrogate_keys:
  true`, with `surrogate_keys_enabled: false`, or without an active surrogate key column.
  The SK merge now does real SCD2 versioning (SK + `Audit_EffectiveDate`) and needs a
  framework-generated SK.
- Folder-HWM mode with a format that is not big-endian (`y, M, d, H, m, s, S` order), with a
  single-letter non-year field (`M`, `d`, `H`, ...; use `MM`, `dd`, `HH`), or with
  `use_auto_loader: true`. Outside folder-HWM mode the first two are generation warnings.

**Action required**

1. Run `biontoolkit validate` and fix any new errors, then regenerate and redeploy.
2. SCD2 tables with `change_hash_enabled`: the old merge inserted a duplicate current row for
   unchanged keys on every run. Check
   `SELECT <keys>, count(*) FROM <table> WHERE Audit_RowIsCurrent GROUP BY <keys> HAVING count(*) > 1`.
   In parameterized tables these duplicates are not repaired by the new merge; reload those
   tables.
3. Models that set `iso_folder_name_as_effective_date: true` on a populated SCD2 table whose
   rows already carry real (processing-time) effective dates: choose remedy (a) or (b) above
   before the first run. (Legacy rows with NULL SCD2 columns are initialised to
   `1900-01-01` and do not need this.)
4. Expect a longer first run for ISO + file-level HWM tables (catch-up) and for per-table
   Type 1 / SCD2 tables with `change_hash_enabled` (one-time rewrite or extra version).
5. Non-SCD2 tables with `not_in_source_deletes_enabled` + `hard_deletes: false`: rename any
   user column named `Audit_IsDeleted`, and add `coalesce(Audit_IsDeleted, false) = false` to
   consumers that must hide deleted rows. The column itself is added automatically.

---

## Settings Reference

All settings live under `settings:` in `settings.yaml`. The four sections are `source`, `destination`, `processing`, and `deployment`.

### `source` — Source Configuration

Controls where and how raw data is read.

#### Cloud / storage

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cloud_provider` | `azure` \| `aws` | `azure` | Source storage cloud. |
| `source_type` | `file` \| `table` | `file` | Whether to read from raw files or existing Delta/Hive tables. |
| `use_unity_catalog_volumes` | bool | `false` | Mount source via Unity Catalog Volumes path instead of `abfss://` or `s3://`. |
| `volume_name` | string | `""` | Volume name when `use_unity_catalog_volumes: true`. |
| `source_catalog` | string | `""` | Catalog containing the source database (table-based or volume path). |
| `source_database` | string | `""` | Database/schema containing the source tables or volume. |
| `storage_account` | string | `""` | Azure storage account name (file-based, `cloud_provider: azure`). |
| `container` | string | `""` | Azure container (file-based, `cloud_provider: azure`). |
| `folder_path` | string | `""` | Path within container or volume to the source data root. |
| `s3_bucket` | string | `""` | S3 bucket name (file-based, `cloud_provider: aws`). |
| `s3_folder_path` | string | `""` | S3 key prefix / folder path (file-based, `cloud_provider: aws`). |
| `source_secret_scope` | string | `""` | Databricks secret scope holding storage credentials. |

#### File structure

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `file_type` | `parquet` \| `csv` \| `json` | `parquet` | Source file format. |
| `file_extension` | string | `""` | File extension including the dot, e.g. `.parquet`. Used in DDL comments. |
| `file_pattern` | string | `""` | Glob pattern / filename prefix for the source files, applied as a path-suffix glob on `.load(...)`. Supports tokens: `<tablename>`, `<catalogname>`, `<schemaname>`. For generated notebooks, defaults to `<tablename>_*` when empty; for DLT, an empty pattern reads the whole folder (no filter). See [How `file_pattern` is applied](#how-file_pattern-is-applied). |
| `file_pattern_is_regex` | bool | `false` | Declared flag indicating `file_pattern` is a regex. The generators currently apply `file_pattern` as a path-suffix glob, so use glob syntax (e.g. `Customer_*`). |
| `per_table_subfolder` | bool | `false` | When `true`, appends `/{table_name}/` to the source path for each table. Combine with `file_pattern` to isolate by directory *and* filename. |
| `folder_traversal_depth` | int | `1` | How many subdirectory levels to search for files. |
| `source_force_string_types` | bool | `false` | Read all columns as STRING to prevent schema-inference type mismatches. |
| `source_sanitize_column_names` | bool | `false` | Normalise source column names (strip special chars) at read time. |
| `source_most_recent_file_only` | bool | `false` | Sort files by modification time and load only the latest one. |
| `source_most_recent_file_per_name` | bool | `false` | For each distinct filename stem, load only the most recent file. |

#### How `file_pattern` is applied

For **file-based** sources (`source_type: file`) the resolved `file_pattern` is appended to the source path as a glob suffix and passed to `.load(...)`. This applies to the standard `spark.read` notebook read, the Auto Loader (`spark.readStream` + `cloudFiles`) notebook read, and — **as of this version** — the **DLT / Lakeflow** generator, which now applies the same filter to *both* its Auto Loader streaming read and its non-streaming batch read. A shared-folder layout therefore no longer cross-scans other tables' files in DLT pipelines.

- **Resolution order:** per-table `source_file_pattern` (table YAML) → model-level `file_pattern`. For generated notebooks an unset pattern falls back to the `<tablename>_*` default; in **DLT** an unset pattern leaves the read **unfiltered** (whole folder), preserving existing behaviour.
- **Tokens:** `<tablename>`, `<catalogname>`, `<schemaname>` are substituted at generation time (case-insensitive).
- **`per_table_subfolder` vs `file_pattern`:** `per_table_subfolder: true` isolates each table by *directory* (`.../{table_name}/`); `file_pattern` filters by *filename* within a (possibly shared) folder. They are independent and can be **combined** — e.g. a per-table subfolder whose files are still named per source can be narrowed further with a pattern.
- `file_pattern_is_regex` is a declared flag; generation applies `file_pattern` as a path-suffix glob, so use glob syntax (e.g. `Account_*`).

```yaml
settings:
  source:
    source_type: file
    use_auto_loader: true
    file_pattern: "<tablename>_*"   # model-level default for every table
# tables/Account.yaml
settings:
  source_file_pattern: "Account_2024_*"   # per-table override wins for this table
```

#### ISO-dated folders

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `use_iso_dated_folders` | bool | `false` | Source files sit in timestamp-named subfolders. By default only the most recent folder is read; with `use_high_watermark: true` and file-level HWM every pending folder is processed in order (see below). |
| `iso_date_folder_format` | string | `yyyyMMddHHmmss` | `SimpleDateFormat` pattern for the dated subfolder names. Only numeric fields are supported (a name must fully match, e.g. `yyyyMMddHHmm` matches exactly 12 digits). Must be big-endian (`y, M, d, H, m, s, S` order) in folder-HWM mode. Use fixed-width fields (`MM`, `dd`, `HH`, ...): a single-letter non-year field matches exactly one digit; it is a validation error in folder-HWM mode and a generation warning in newest-folder-only mode. |
| `iso_folder_name_as_effective_date` | bool | `false` | Use the ISO folder name (parsed with `iso_date_folder_format`, in the Spark session time zone) as `Audit_EffectiveDate` instead of the processing time. Requires `processing.effective_dates_enabled: true` and `use_iso_dated_folders: true` (otherwise it is ignored with a generation warning). Works in folder-HWM mode and in newest-folder-only mode. In both modes the run fails if the folder's date is older than the newest current `Audit_EffectiveDate` already in the target (an equal date and a missing or empty target are allowed). In newest-folder-only mode this means the newest folder was removed or rolled back: restore the folder or reload the table. For folder-HWM mode see the stale-folder guard below. This flag did nothing before the ISO-folder HWM release; enabling it (or upgrading a model that already sets it) on a populated SCD2 table usually trips this guard on the first run, see [Upgrade notes](#upgrade-notes). A folder name that cannot be parsed fails the run. With `effective_dates_round_to_midnight: true` the folder timestamp is truncated to `00:00:00`. |
| `max_folders_per_run` | int | `0` | Folder-HWM mode only: cap on the number of pending folders one run processes (`0` = unlimited). The next run continues; the exit message reports `remaining=M` (pending folders that have files for the table; empty folders are not counted). |
| `iso_folder_initial_load` | `all` \| `latest` | `all` | Folder-HWM mode only, first run only (no `__iso_folder__` row and no derivable legacy `__file_path__` row): `all` processes every matching folder oldest first; `latest` processes only the newest non-empty matching folder. |

**Folder-HWM mode** is active when `use_iso_dated_folders`, `use_high_watermark` and `high_watermark_file_level` are all `true` on a file-based source (not supported with `use_auto_loader`, and not in the `consolidated` layout, which keeps newest-folder-only behaviour). The table notebook then runs as a *driver* (no `iso_folder` widget value): it lists the pending dated folders under the source root, oldest first, and runs the same notebook once per folder as a *child* (`iso_folder=<name>`). The first failing folder stops the run and is re-raised; a retry resumes there.

- Only directories whose names fully match `iso_date_folder_format` are considered; root files and other directories (e.g. `_archive`) are ignored (the count is printed).
- The watermark is **per table**, stored as folder name under `ColumnName = '__iso_folder__'` and committed after each folder's MERGE succeeds. It never moves backwards, and a child refuses a folder at or below it.
- **Empty-folder rule:** a folder with no files for this table (after `file_pattern`) does not advance the watermark and is rechecked on the next run. If a later folder loads files, the watermark advances past the empty one.
- **Upgrade / auto catch-up:** existing `__file_path__` watermarks are read (never modified) and the last path segment matching the folder format becomes the starting folder. Tables that previously read only the newest folder will now also process every folder newer than that watermark on the first run; set `iso_folder_initial_load: latest` (new tables only) or `max_folders_per_run` to limit this. If no segment matches, the table is treated as having no watermark.
- Late files added to an already committed folder are ignored; to reprocess the **newest loaded folder**, reset the `__iso_folder__` row to the previous folder and rerun.
- **Stale-folder guard (SCD2 + `iso_folder_name_as_effective_date`):** a child run raises when its folder date is *earlier* than the newest current `Audit_EffectiveDate` in the target (a missing/empty target and an equal date are allowed). Typical cause: existing rows were dated by processing time before the flag was enabled. Remedy: reload the table (empty or drop the target **and** delete BOTH the table's `__iso_folder__` and `__file_path__` rows; a remaining legacy `__file_path__` row would otherwise set the restart point; then run the driver so every folder is processed oldest first). Resetting the `__iso_folder__` row alone only supports reprocessing the newest loaded folder; it cannot reprocess older folders.
- RunAllNotebooks runs these tables with `timeout_seconds=0` (a driver can span many folders); child runs use `processing.notebook_run_timeout`. `loop_until_no_files_left` and `source_most_recent_file_only` are ignored (generation warning).
- **`not_in_source_deletes_enabled` is the snapshot / delta switch.** Each child run loads exactly one folder, so it is evaluated per folder: turn it **on** when every folder is a complete snapshot (rows missing from the folder are deleted, `hard_deletes` true = physical DELETE, false = soft delete: SCD2 expires the current row, a non-SCD2 table sets `Audit_IsDeleted`), and leave it **off** when folders are deltas (otherwise every row not in the delta would be deleted). `use_high_watermark` together with `not_in_source_deletes_enabled` is rejected outside folder-HWM mode, but allowed here. A folder with no files for the table (after `file_pattern`) is skipped and never triggers a delete-all (the watermark does not advance). A folder that has files but whose files hold **zero rows** is a real empty snapshot: in folder-HWM child runs the empty-source safety guard is bypassed, every current row is soft- or hard-deleted per `hard_deletes` (with SCD2 only current rows; expired history is never touched), and the watermark then advances and the run continues. Every other mode (including newest-folder-only) keeps the guard and fails on an empty source when `not_in_source_deletes_enabled` is on.
- **Same-day semantics (SCD2 + `iso_folder_name_as_effective_date`):** each folder produces versions dated by its own folder timestamp, so folders are applied in order. With `effective_dates_round_to_midnight: true`, several folders on the same date share one `Audit_EffectiveDate`; the **last folder of the day wins** and updates the current version in place (no extra version row). The SCD2 expiry step only expires versions whose `Audit_EffectiveDate` is *earlier* than the incoming one, so reprocessing a folder (or running the same notebook twice on one day) is idempotent instead of failing the MERGE. With `change_hash_enabled`, a key whose current row has the same `ChangeHash` is left untouched (no new version, and it is never flagged by `not_in_source_deletes_enabled`); a changed key expires the old current row (`Audit_ExpiredDate` = the new effective date, `Audit_RowIsCurrent` = false) and inserts a new current version. `not_in_source_deletes_enabled` is applied as a second statement over the full snapshot and only deletes (or soft-deletes, setting `Audit_RowIsCurrent` = false and `Audit_ExpiredDate`) CURRENT rows whose key is absent from the source. Without `change_hash_enabled` every matched key is treated as changed, except in a key-only table (no non-key business columns), where an existing current key is never re-versioned. With the flag off each folder is dated at processing time.

#### Incremental / streaming

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `use_auto_loader` | bool | `false` | Use Databricks Auto Loader (`cloudFiles`) for incremental file ingestion. |
| `use_high_watermark` | bool | `false` | Maintain a `HighWatermark` table and filter the source to only rows newer than the last run. |
| `high_watermark_file_level` | bool | `true` | When `use_high_watermark: true`, track watermark by file path (lexicographic) rather than by column value. Set `false` for column-based watermark. With `use_iso_dated_folders: true` on a file source this enables ISO-folder HWM mode, which tracks the last processed folder instead (see [ISO-dated folders](#iso-dated-folders)). |
| `process_one_file_per_run` | bool | `false` | Load exactly one file per run, advancing the watermark each execution. |
| `loop_until_no_files_left` | bool | `false` | Repeat file loads until an iteration loads no new files. `per_table`: RunAllNotebooks runs the companion `<prefix><Table>_Loop` notebook; `parameterized`: RunAllNotebooks loops the ProcessTable call; `consolidated`: unsupported (warning). Stops on exit `no_files` or `files=0` (file-level HWM tables exit `success: rows_after=N files=<n>`, `n` = files loaded after the watermark filter). It can only make progress with `use_high_watermark` + `high_watermark_file_level`, or a `processing.post_run_notebook_path` that moves processed files; otherwise generation warns and every iteration reloads the same files. Without file-level HWM, each iteration first lists the source folder (honouring `file_pattern`) and exits `no_files` when nothing matches, so a post-run archive loop ends cleanly once the folder is drained. `source_most_recent_file_only` is ignored with file-level HWM. Ignored in ISO-folder HWM mode. |
| `max_loop_iterations` | int | `100` | Maximum loop repetitions when `loop_until_no_files_left: true`; a warning is printed if reached without a stop signal. |
| `overwrite_processed_files` | bool | `false` | Overwrite source files with a processed marker after a successful load. |

#### CSV options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `csv_delimiter` | string | `,` | Column separator character. |
| `csv_quote_char` | string | `"` | Quote character surrounding fields that contain the delimiter. |
| `csv_has_header` | bool | `true` | First row of each CSV file is a header row. |
| `csv_rows_to_skip` | int | `0` | Number of non-header rows to skip at the start of each file. |

#### JSON options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `json_multiline` | bool | `false` | Each JSON file contains a single multi-line JSON object (not newline-delimited). |
| `json_auto_expand` | bool | `false` | Automatically flatten nested JSON objects into dot-separated columns. |
| `json_level_delimiter` | string | `_` | Separator character used when flattening nested JSON keys. |

---

### `destination` — Destination Configuration

Controls where generated Delta tables are created and registered.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `zone` | `bronze` \| `silver` \| `gold` | `silver` | Medallion zone label — descriptive only, does not restrict features. |
| `dest_catalog` | string | **required** | Unity Catalog catalog name (e.g. `dev_catalog`). |
| `target_database` | string | **required** | Database/schema within the catalog (e.g. `silver`). |
| `managed_tables` | bool | `true` | When `true`, tables are Unity Catalog managed (no external storage path). Set to `false` for external/unmanaged tables. |
| `dest_cloud_provider` | `azure` \| `aws` | `azure` | Destination storage cloud (used when `managed_tables: false`). |
| `dest_storage_account` | string | `""` | Azure destination storage account. Supports `<ENV:VAR>` tokens. |
| `dest_container` | string | `""` | Azure destination container. |
| `dest_folder_path` | string | `""` | Path prefix within the destination container/bucket. |
| `dest_s3_bucket` | string | `""` | AWS destination S3 bucket (when `dest_cloud_provider: aws`). |
| `dest_s3_folder_path` | string | `""` | AWS destination S3 key prefix. |
| `dest_subfolder` | string | `""` | Additional subfolder appended to the destination path. |
| `dest_secret_scope` | string | `""` | Databricks secret scope for destination storage credentials. |
| `create_auto_view` | bool | `false` | Generate a `CREATE OR REPLACE VIEW` wrapping each target table. |
| `add_primary_key_constraints` | bool | `false` | Emit `ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY` DDL (Unity Catalog). |
| `auto_schema_enabled` | bool | `false` | Allow the target schema to evolve automatically as columns are added. |
| `view_only_option_enabled` | bool | `false` | Generate views instead of managed Delta tables for the whole model. |
| `dest_catalog_suppress_create` | bool | `false` | Do not emit `CREATE CATALOG IF NOT EXISTS` DDL in `UpdateSchema`. |
| `dest_database_suppress_create` | bool | `false` | Do not emit `CREATE SCHEMA IF NOT EXISTS` DDL in `UpdateSchema`. |

---

### `processing` — Processing Options

Feature flags that control which ETL capabilities are generated. All default to `false` unless noted.

#### Custom code cells

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `beginning_cells_enabled` | bool | `false` | Generate a `beginning_cell` stub at the start of each table notebook. Populated per table via `table.settings.beginning_cell`. |
| `pre_query_cells_enabled` | bool | `false` | Generate a `pre_query_cell` stub before the MERGE. |
| `end_cells_enabled` | bool | `false` | Generate an `end_cell` stub after MERGE, audit, and statistics. |

#### Surrogate / hash keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `surrogate_keys_enabled` | bool | `false` | Compute a SHA2 hash surrogate key column from the source key columns. |
| `passthrough_surrogate_keys` | bool | `false` | Copy the surrogate key value from a source column instead of computing it. |
| `merge_on_surrogate_key` | bool \| null | `null` | When `true`, MERGE ON uses the surrogate key column. When `false`, uses source key columns. `null` applies the generator default. With `effective_dates_enabled` (SCD2) the SK path versions on SK + `Audit_EffectiveDate` (all versions of a business key share the generated SK) and requires `surrogate_keys_enabled: true`, `passthrough_surrogate_keys: false` and an active surrogate key column; other combinations are validation errors. |
| `hash_function` | string | `UNHEX(SHA2(<stringvalue>, 256))` | SparkSQL hash function template. `<stringvalue>` is replaced with the CONCAT expression. The default stores the raw 32-byte SHA-256 digest in the BINARY key / ChangeHash columns; changing this template changes every stored hash value, so existing tables must be reloaded. |
| `hash_multi_value_delimiter` | string | `\|` | Delimiter character inserted between source key values in the hash CONCAT. |
| `global_null_key_replacement_function` | string | `IFNULL(<value>, 'NULL')` | Null replacement applied to each column in the hash CONCAT (surrogate keys, foreign keys, integer keys and `ChangeHash`). `<value>` receives the column already cast to STRING, so the default renders `IFNULL(CAST(col AS STRING), 'NULL')` and a NULL in a non-string column hashes correctly with ANSI mode on. Changing it changes the hash of every row with a NULL in a hashed column. |
| `int_surrogate_keys_enabled` | bool | `true` | Generate an integer (BIGINT) companion column alongside every computed binary surrogate/foreign-surrogate key. |
| `int_key_hash_function` | string | `xxhash64(<stringvalue>)` | SparkSQL hash function template applied to the same concatenated source-key expression as `hash_function`, producing the BIGINT integer companion key. The integer key is always this function; when an int companion is generated, the concatenated pre-image is computed once into a scratch column (`__bion_hash_preimage`) shared by the binary and integer hashes, then dropped before the data reaches the target table. A foreign key's integer key hashes the original FK source value, so it equals the referenced dimension's integer surrogate key. |
| `hash_int_conversion_function` | string | `` (empty) | **Deprecated and ignored.** The old `CONV(HEX(<binary key>), 16, 10)` derivation overflowed on 32-byte digests (every integer key collapsed to one value, or `ARITHMETIC_OVERFLOW` under ANSI) and has been retired. Existing YAML that sets it still validates, but a warning is printed at generation time and the integer key is `int_key_hash_function`. Remove it from `settings.yaml`; integer key values change versus the old path. |
| `int_key_identifier` | string | `_Id` | Suffix appended to a surrogate/foreign surrogate-key column's name to form its integer companion key's column name (e.g. `<SK>` → `<SK>_Id`). Blank means no integer keys are generated. |

**Column auto-synthesis:** when `surrogate_keys_enabled: true`, you do not need to
manually declare the surrogate key column or its integer companion column(s) — they
are added automatically when the model loads:

- If no column is flagged `is_surrogate_key: true` and `passthrough_surrogate_keys` is
  not set, a column named `SurrogateKey` (`BINARY`, `is_surrogate_key: true`) is added.
- If `int_surrogate_keys_enabled` is on and `int_key_identifier` is non-empty, an
  integer companion column is added for the surrogate key (`<SurrogateKeyName><int_key_identifier>`,
  `BIGINT`) and for every FK column (`foreign_table` set, no custom `expression`)
  (`<FKColumnName><int_key_identifier>`, `BIGINT`). Not added for the surrogate key's
  own companion under `passthrough_surrogate_keys`; FK companions are unaffected by
  that flag.

These rules define exactly which integer key columns the engine generates. The same set
is used by every generator, by `FixIntCollisions` (which skips tables without an integer
key) and by the `ChangeHash` exclusion. A column you declare that only looks like an
integer key (e.g. `SK_Customer_Id` while `int_surrogate_keys_enabled` is off) is an
ordinary business column.

You can still declare any of these columns yourself — a custom name for the surrogate
key (via `is_surrogate_key: true`), or a different data type for an `_Id` companion —
and your declaration is always honored as-is, never overwritten or duplicated. A column
literally named `SurrogateKey` without `is_surrogate_key: true`, in a model where
auto-synthesis would otherwise claim that name, is a validation error — flag it as the
surrogate key or rename it.

#### Effective dates (SCD2)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `effective_dates_enabled` | bool | `false` | Generate SCD Type 2 logic. Each version is identified by its merge key plus `Audit_EffectiveDate`. A changed key expires the current row (`Audit_ExpiredDate` = the new effective date, `Audit_RowIsCurrent` = false) and inserts a new current version; a change arriving with the same effective date as the current version updates that version in place; with `change_hash_enabled` an unchanged key is left untouched. Only versions with an earlier effective date are expired, so re-running a batch is idempotent. With `not_in_source_deletes_enabled` the delete step runs as a second MERGE over the full source and touches CURRENT rows only. NULL merge-key values are not supported: the SCD2 merges and the legacy-row initialisation match keys with `=`, so a NULL key never matches its own history. |
| `effective_dates_round_to_midnight` | bool | `false` | Truncate `Audit_EffectiveDate` to `00:00:00` when set from a timestamp source. |
| `effective_dates_from_iso_folder` | bool | `false` | **Deprecated alias** of `source.iso_folder_name_as_effective_date`: when `true` it is ORed into that flag and a deprecation warning is printed. Use the `source` flag. |

#### Change hash

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `change_hash_enabled` | bool | `false` | Compute a `ChangeHash` (`BINARY`) column over the non-key business columns and use it for change detection. It uses `hash_function` and the same CONCAT / null replacement as the keys. It covers real source business columns only: source keys, the surrogate key, FK columns (`foreign_table` set), the high-watermark column, `Audit_*` columns (any case), the `ChangeHash` column and the integer key columns the engine generates are excluded; columns with an `expression` are included. It is never part of the MERGE `ON` condition. The hash is persisted in the target in every notebook layout and compared null-safely (`<=>`), so a NULL stored hash counts as changed. Type 1: a matched row is updated only when its hash differs. SCD2: a key whose current row has the same hash is left untouched (no new version); a different hash expires the current row and inserts a new version. A table with no non-key business columns (key-only, e.g. a bridge table, including one whose only other columns are generated integer keys) has nothing to hash: the setting is ignored for that table with a generation warning and no `ChangeHash` column is created. Without it, every matched row is updated (Type 1) or re-versioned (SCD2); a key-only SCD2 table is the exception: an existing current key is left untouched and only new keys (or keys reappearing after a delete) get a version. |

#### Delete processing

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `not_in_source_deletes_enabled` | bool | `false` | Treat the source as a full snapshot: target rows whose key is absent from the source are deleted (`WHEN NOT MATCHED BY SOURCE`). Type 1: a clause of the main MERGE. SCD2: a separate second MERGE over the full source that only touches CURRENT rows (expired history is never deleted or changed). The run fails if the source is empty (safety guard), except for a zero-row folder in ISO-folder HWM mode, which is a real empty snapshot. Rejected together with `use_high_watermark` (a filtered source is not a snapshot), except in ISO-folder HWM mode. |
| `hard_deletes` | bool | `true` | When `not_in_source_deletes_enabled: true`: `true` = DELETE the row (SCD2: the current row only), `false` = soft delete. SCD2 (`effective_dates_enabled: true`): the current row gets `Audit_RowIsCurrent = false` and `Audit_ExpiredDate` = the batch's effective date. Non-SCD2: the table gets an `Audit_IsDeleted BOOLEAN` column; a key missing from the source is flagged `true` once (with `Audit_ModifiedDateTime` updated), a key that reappears is set back to `false` even if its `ChangeHash` is unchanged, and inserts write `false`. NULL (rows written before the column existed) means not deleted, so filter with `coalesce(Audit_IsDeleted, false) = false` to hide deleted rows. The flag is not applied with `truncate_and_load` or in DLT pipelines. See [System-injected audit columns](#system-injected-audit-columns). |

#### Data quality expectations

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `expectations_enabled` | bool | `false` | Enable row-level data quality evaluation. Expectations are defined per table (see [Table Settings](#table-yaml--table-settings)). |

#### Table maintenance

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `include_create_statements` | bool | `true` | Emit `CREATE TABLE IF NOT EXISTS` DDL at the top of each notebook and in `UpdateSchema`. |
| `vacuum_enabled` | bool | `false` | Append a `VACUUM` command after each notebook run. |
| `vacuum_retention_hours` | int | `168` | Retention period (hours) passed to `VACUUM … RETAIN N HOURS`. |
| `optimize_enabled` | bool | `true` | Append an `OPTIMIZE` (and `ZORDER BY` where applicable) command after each run. |
| `table_statistics_enabled` | bool | `true` | Write row-count and run-timestamp to `Audit_RuntimeTableLog` after each MERGE. |
| `infer_missing_records_enabled` | bool | `false` | Generate `InferMissingRows` logic to insert placeholder rows for un-resolvable foreign keys. The placeholder ("Unknown") row gets the binary surrogate key and, when both the fact's FK and the dimension have integer keys and the dimension is in the same model, the integer key too, so facts join to it on either key. |
| `truncate_and_load` | bool | `false` | Truncate the target table before each load instead of performing a MERGE. With file-level HWM (`use_high_watermark` + `high_watermark_file_level`, non-ISO) the truncate is skipped on a run that loads no new files (every file is already past the watermark), so an empty incremental batch, including the final iteration of `loop_until_no_files_left`, never empties the table. Without file-level HWM an empty source still truncates (reload as empty); ISO-folder mode truncates per folder snapshot. |
| `dedup_source_enabled` | bool | `false` | Deduplicate `source_df` by merge key(s) before the MERGE runs. Prevents `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` when the source extract contains duplicate rows. Can be overridden per table using `dedup_source_enabled` in the table YAML. |
| `dedup_source_recency_column` | string | `""` | Column used to determine the most-recent row when deduplicating (e.g. `SystemModstamp`, `ModifiedDate`). Uses `Window.partitionBy(*merge_keys).orderBy(col.desc_nulls_last())` — most-recent row is kept. When empty, falls back to `dropDuplicates(merge_keys)` which is non-deterministic but safe. Ignored when `dedup_source_enabled` is `false` or `truncate_and_load` is `true`. Can be overridden per table using `dedup_source_recency_column` in the table YAML. |

#### Spark / Delta config

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `ansi_mode` | bool | `false` | Emit `SET spark.sql.ansi.enabled = true` at the start of each notebook. |
| `add_grave_accent` | bool | `false` | Wrap column names in backticks in generated SQL to handle reserved words and special characters. |
| `change_data_feed_enabled` | bool | `false` | Emit `delta.enableChangeDataFeed = true` in table properties DDL. |
| `delta_auto_optimize_write` | bool | `false` | Emit `delta.autoOptimize.optimizeWrite = true` in table properties DDL. |

#### Streaming (DLT)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `streaming_enabled` | bool | `false` | Generate a Delta Live Tables (DLT) pipeline file instead of standard notebooks. Takes precedence over `artifact_type`. |
| `streaming_persist_source` | bool | `false` | When `true`, the CDC path emits `create_streaming_table` + `create_auto_cdc_flow` as module-level calls (required for SCD1/SCD2 CDC pipelines). Automatically enabled when `effective_dates_enabled: true`. |
| `streaming_use_cdf_source` | bool | `false` | **TABLE_BASED sources only.** When `true`, the CDC path emits an intermediate `@dp.view` that reads the source Delta table via Change Data Feed (`readChangeFeed: true`), filters out `update_preimage` rows, and renames the CDF metadata columns to `_source_change_type`, `_source_commit_version`, `_source_commit_timestamp`. `create_auto_cdc_flow` then sources from this view, with `sequence_by=F.col("_source_commit_timestamp")` and `apply_as_deletes=F.expr("_source_change_type = 'delete'")` overriding any configured `streaming_sequence_column`/`streaming_cdc_delete_expression`. Requires the source Delta table to have CDF enabled. Ignored (with a WARNING comment) for FILE_BASED sources. |
| `dlt_quarantine_enabled` | bool | `False` | When `True` (requires `expectations_enabled: true`): for each non-CDC `@dp.table`, also emit a companion `quarantine_{TableName}` `@dp.table` reading from an intermediate `@dp.view`. Activated by `write_and_audit` or `audit_only` expectations (not `drop`). The quarantine table captures rows where at least one of those expressions failed, using the inverted expression `NOT((expr1) AND (expr2))`. CDC path (`streaming_persist_source: true`) is excluded. See [Expectation actions — DLT path](#expectation-actions) below. |

#### Parameterized notebook orchestration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `parallel_notebook_workers` | int | `4` | Number of threads in the `ThreadPoolExecutor` used by `RunAllNotebooks` when `artifact_layout: parameterized`. Increase for models with many tables. |
| `notebook_run_timeout` | int | `3600` | Per-table timeout in seconds that `RunAllNotebooks` passes to `dbutils.notebook.run()`; `0` = no timeout. Tables in ISO-folder HWM mode are run by RunAllNotebooks with `0` (the driver can span many folders), and each per-folder child run uses this value (`0` is honoured there too). |
| `metadata_json_files_enabled` | bool | `false` | When `true` (and `artifact_layout: parameterized`), the generator writes a `{table_name}_config.json` file to the dependency folder for each table. `RunAllNotebooks` reads these files at runtime using `open('/Workspace{dep_path}/{table_name}_config.json')` instead of using the JSON strings embedded inline. When `false` (default), config is embedded in `RunAllNotebooks` at generation time. See [Metadata JSON files](#metadata-json-files-parameterized-layout) below. |

#### Code generation

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `settings_dump_enabled` | bool | `false` | Inject a cell that prints the full resolved `_cfg` dict at runtime — useful for debugging generated config. |
| `include_debug_cells` | bool | `false` | Inject additional diagnostic cells (counts, schema prints) throughout the notebook. |
| `delete_table_data_in_debug_mode` | bool | `false` | When **both** this and `include_debug_cells` are `true`, injects a debug cell that runs `DROP TABLE IF EXISTS` on the destination table before processing begins. Intended for dev reset workflows only — never enable in production. |
| `explicit_casting_enabled` | bool | `false` | Wrap every non-expression, non-surrogate source column in `CAST(col AS type)` in the MERGE USING clause. Forces type errors to raise immediately rather than silently producing nulls. |
| `pre_run_notebook_path` | string | `""` | Databricks workspace path to a notebook that runs **before** table processing begins (via `dbutils.notebook.run(path, timeout_seconds=0)`). Empty string = disabled. |
| `post_run_notebook_path` | string | `""` | Databricks workspace path to a notebook that runs **after** all processing, OPTIMIZE, and VACUUM steps (via `dbutils.notebook.run(path, timeout_seconds=0)`). Empty string = disabled. |
| `add_batch_notebooks` | bool | `false` | Generate batch wrapper notebooks that group table notebooks into numbered batches for sequential execution. |

---

### System-injected audit columns

The following columns are automatically added to **every generated table** — no YAML setting is required and they cannot be disabled. Do not declare them in your table YAML; they will be injected regardless.

| Column | Type | INSERT value | UPDATE value |
|--------|------|-------------|-------------|
| `Audit_CreatedDateTime` | `TIMESTAMP` | `current_timestamp()` | **Not updated** — preserves the original insert time |
| `Audit_ModifiedDateTime` | `TIMESTAMP` | `current_timestamp()` | `current_timestamp()` |

These columns are excluded from `ChangeHash` computation (along with all other `Audit_*` columns). They are present in all layouts: `per_table`, `consolidated`, `parameterized`, and DLT.

**Soft-delete flag (conditional).** `Audit_IsDeleted BOOLEAN` is added (after `Audit_ModifiedDateTime`) only when `not_in_source_deletes_enabled: true`, `hard_deletes: false` and `effective_dates_enabled: false`, and not with `truncate_and_load`; notebook layouts only. While it is active `Audit_IsDeleted` is a reserved column name (validation error); use `source_name` to read a source column of that name under a different name. A flagged row stays in the table: InferMissingRows treats it as an existing dimension member (no Unknown row is inserted for an FK pointing at it) and referential-integrity views still match it, so consumers that must hide deleted rows filter `coalesce(Audit_IsDeleted, false) = false`. TableStatistics `RowsDeleted` counts physical deletes only; soft deletes are counted in `RowsUpdated`. In the parameterized layout the `table_configs` JSON carries `soft_delete_flag` (and `key_only`, for tables with no non-key business columns), both defaulting to `false`.

---

### `deployment` — Deployment Configuration

Controls where generated notebooks are written and how they are deployed.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `artifact_type` | `notebook` \| `dlt` | `notebook` | Output format. `dlt` generates a Delta Live Tables pipeline file. `streaming_enabled: true` also selects DLT. |
| `artifact_layout` | `per_table` \| `parameterized` \| `consolidated` | `per_table` | Controls how notebooks are structured — see comparison below. **Notebook path only**; ignored for DLT. |
| `dlt_layout` | `consolidated` \| `per_table` | `consolidated` | **DLT path only** (`artifact_type: dlt` or `streaming_enabled: true`); ignored on the notebook path. `consolidated` (default) emits a single `{ModelName}_pipeline.py` containing every `@dlt.table`; `per_table` emits one self-contained `{TableName}.py` per active table. Any unknown/unset value falls back to `consolidated`. The resulting pipeline DAG is **identical** either way — the DLT runtime globs the pipeline source root and registers every `@dlt.table` regardless of file count, so this is an authoring/maintainability choice only. |
| `dlt_import_style` | `lakeflow` \| `legacy` | `lakeflow` | **DLT path only**; ignored on the notebook path. `lakeflow` (default): emits `from pyspark import pipelines as dp` and uses `@dp.table`, `dp.read_stream`, etc. (requires DBR 15.4 LTS+). `legacy`: emits `import dlt` and uses `@dlt.table`, `dlt.read_stream`, etc. (compatible with all DLT-capable runtimes). Any unrecognised value falls back to `lakeflow`. |
| `local_deployment_enabled` | bool | `true` | Write generated files to `local_process_path` / `local_dependency_path`. |
| `local_process_path` | string | `""` | **Local output path** where table notebooks are written on the developer's machine. Supports `<ENV:VAR>` tokens (e.g. `<ENV:REPO_ROOT>/notebooks/silver/tables`). This path is **never baked into generated notebooks** — only the relative offset between `local_process_path` and `local_dependency_path` is used at runtime. Both paths must share a common prefix (or common `<ENV:...>` prefix) so the generator can compute a correct relative `_proc_offset` such as `../tables`. |
| `local_dependency_path` | string | `""` | **Local output path** where shared notebooks (`RunAllNotebooks`, `Functions`, `UpdateSchema`) are written. Must share a common prefix with `local_process_path`. Supports `<ENV:VAR>` tokens. Never used at Databricks runtime — all paths in deployed notebooks are fully relative. |
| `api_deployment_enabled` | bool | `false` | Deploy to Databricks via REST API in addition to local write. |
| `databricks_workspace_url` | string | `""` | Databricks workspace URL for API deployment (e.g. `https://adb-123.azuredatabricks.net`). |
| `remote_process_path` | string | `""` | Workspace path for table notebooks on Databricks (e.g. `/Users/you@company.com/project/silver/notebooks`). Required for API deployment. |
| `remote_dependency_path` | string | `""` | Workspace path for shared notebooks on Databricks. Required for **API deployment** (`api_deployment_enabled: true`). Not required for runtime execution — when `metadata_json_files_enabled: true`, `RunAllNotebooks` resolves `dep_path` from its own workspace location at runtime, so no absolute path config is needed. |
| `bundle_enabled` | bool | `false` | When `true`, `generate` and `deploy --target local` also produce a Databricks Asset Bundle — see [Databricks Asset Bundle (DAB) output](#databricks-asset-bundle-dab-output) below. Off by default (classic local deployment). `--bundle` on the CLI forces it on and `--classic` forces it off for a single run. |
| `bundle_cluster_policy_id` | string | `""` | Databricks cluster policy ID applied to the compute block of generated bundle job/pipeline resources (`new_cluster: {policy_id: ...}` for the job resource, `clusters: [{label: default, policy_id: ...}]` for a DLT pipeline resource). When empty (default), no compute block is emitted and the resource runs on serverless compute. Only meaningful when `bundle_enabled: true`. |

> **Shared env module (per-table + environments):** When `dlt_layout == "per_table"` and `model.settings.environments is not None`, a shared env module `{prefix}{ModelName}_env.py` is also emitted in `tables/` alongside the table files. It contains only the environment resolution code (no `@dlt.table` functions). Each per-table file replaces its inlined env cell with `from {prefix}{ModelName}_env import *`. The env module **MUST** be deployed together with the table files — deploying table files without the env module causes `ModuleNotFoundError` at pipeline startup. When `environments is None`, each table file remains self-contained; no env module is emitted.

### Databricks Asset Bundle (DAB) output

Bundles are **off by default**. When `bundle_enabled: true` (or `--bundle` is passed), every `generate` and `deploy --target local` run also writes:

- **`databricks.yml`** — bundle name, `include: [resources/*.yml]`, and one `targets.<name>:` block per `environments:` profile (or a single `default` target when `environments:` is absent). Each target's `workspace.host` is derived from the matching `environments.detection.workspace_urls` entry. For DLT models, each target also carries a per-target `resources.pipelines.<key>.{catalog,target}` override so the same pipeline resource can be deployed against a different catalog/database per environment.
- **`resources/{ModelName}_job.yml`** (notebook path) or **`resources/{ModelName}_pipeline.yml`** (DLT path) — the job or pipeline resource definition. The notebook path always wraps `RunAllNotebooks` in a single task (its own internal per-table fan-out is unchanged, not duplicated at the bundle level); the DLT path references the single consolidated pipeline file, or an explicit list of every per-table file — including the shared env module, when applicable — for `dlt_layout: per_table`.

Both files are written to `local_dependency_path`, alongside `RunAllNotebooks` / `Functions` / `UpdateSchema`.

**Current scope:** this is generation only — BionToolkit does not yet invoke `databricks bundle deploy` / `bundle run` itself; use the Databricks CLI directly against the generated files. `--target api` does not produce bundle files in this version.

**Bundle output never blocks classic generation or deployment.** If bundle generation can't proceed for any reason, a `⚠  Skipping Databricks Asset Bundle generation: ...` message is printed to stderr, and the rest of the run (notebooks, shared artifacts) completes normally. Known warning cases:
- No `pipeline_name` set and no model name available to derive one from.
- A `<ENV:...>` token referenced by `remote_process_path`/`remote_dependency_path` (or their `local_*` fallbacks) has no corresponding environment variable set.
- `local_dependency_path` already contains a `databricks.yml` generated for a **different** model — share `local_dependency_path` only between deploys of the same model when bundles are enabled.

**One exception — `--tables` with `dlt_layout: per_table` is an error.** When bundles are active (`generate`, or `deploy --target local`), `--tables <subset>` on a DLT model with `dlt_layout: per_table` exits with `✗ --tables cannot be used with Databricks Asset Bundle output ...` before anything is generated or deployed, because the resource file's `libraries:` list would contain only the selected tables. Re-run without `--tables`, or drop `--bundle` / add `--classic`. `--tables` is still allowed with bundles for notebook models and DLT `consolidated` layout.

Opt in with `--bundle` on `generate` or `deploy`, or set `bundle_enabled: true` under `deployment:`. `--classic` overrides a YAML `bundle_enabled: true` for a single run.

### Choosing an `artifact_layout`

| Layout | Files generated | Code structure | Use when |
|--------|----------------|----------------|----------|
| `per_table` | One `.py` per table (`Customer.py`, `Address.py`, …) | Each file contains the full ETL logic for that table, with all column names, expressions, and DDL baked in as Python literals at generation time. | Default. Works for any model size. |
| `consolidated` | One `.py` for the whole model (`CustomerConformed.py`) | Same as `per_table` — all tables' full ETL code is still baked in as literals, just concatenated into one file separated by markdown section dividers. No metadata; the code for each table is still hardcoded. | When you want a single Databricks job task or a small model where a single-file deployment is preferred. **Not** metadata-driven. |
| `parameterized` | One `{ModelName}_ProcessTable.py` shared by all tables | A **single notebook whose ETL logic is identical for every table**. All table-specific config (catalog, database, columns, merge keys, feature flags, expectations, DDL) is serialized as a JSON string at generation time and passed as a `dbutils.widgets` argument at runtime. `RunAllNotebooks` calls this one notebook once per table in parallel, each time with a different JSON config. | When you want one notebook deployment that stays unchanged as tables are added/modified — only the config changes. Requires re-running `biontoolkit generate` to update the JSON config embedded in `RunAllNotebooks`. |

> **Key distinction:** `consolidated` still has per-table code sections hardcoded inline — it is not metadata-driven. `parameterized` is the metadata-driven layout: the notebook itself never changes, only the JSON config passed to it does.

To switch to metadata-driven execution, set in `settings.yaml`:

```yaml
deployment:
  artifact_layout: parameterized
  parallel_notebook_workers: 8   # optional: increase for large models
  notebook_run_timeout: 3600     # optional: per-table timeout in seconds
```

---

### Environment Runtime Detection — `environments:` block

The optional `environments:` block enables generated notebooks to auto-detect which Databricks environment they are running in and apply the correct catalog, storage account, and secret scope values at runtime. The same generated files work in dev, test, and prod without modification.

This is the recommended approach for the **generate once → check into repo → DevOps promotes** workflow.

**When `environments:` is absent**, notebooks work exactly as before — all values are baked in as literals.

**When `environments:` is present**, the generator injects a Python cell at the top of every notebook that:
1. Reads `spark.conf.get("spark.databricks.workspaceUrl")` to identify the workspace
2. Maps the URL to an environment name using the `workspace_urls` map you define
3. Falls back to the `BION_ENV` cluster environment variable if the URL is not in the map
4. Falls back to `default_env` (with a `[BION] WARNING` print) if neither is available
5. Sets `catalog`, `database`, `storage_account`, `container`, `volume_name`, `source_catalog`, `source_database`, `source_secret_scope`, `dest_secret_scope`, and the resolved `folder_path` variables from the matched environment profile

All subsequent notebook cells use these variables — no hardcoded environment-specific values.

#### YAML schema

The `environments:` block must be **nested inside `settings:`** — at the same indentation level as `source:`, `destination:`, `processing:`, and `deployment:`.  A common mistake is placing it at the top level of `settings.yaml` (same level as `settings:` itself); the generator silently treats `environments` as absent when it is at the wrong level.

Full `settings.yaml` showing correct placement:

```yaml
model_name: MyModel
zone: silver

settings:                          # ← all model config lives here
  source:
    cloud_provider: azure
    # ... other source fields ...

  destination:
    zone: silver
    # ... other destination fields ...

  processing:
    # ... processing flags ...

  deployment:
    # ... deployment paths ...

  environments:                    # ← MUST be here, inside settings:
    detection:
      method: workspace_url        # workspace_url | env_var | none
      default_env: dev             # used as last-resort fallback; must match a profile name below
      workspace_urls:              # required when method: workspace_url
        adb-111111111111.1.azuredatabricks.net: dev
        adb-222222222222.2.azuredatabricks.net: test
        adb-999999999999.9.azuredatabricks.net: prod

    dev:
      source:
        storage_account: devbronzestorage
        source_catalog:  dev_catalog
        source_database: bronze
        source_secret_scope: dev-kv-scope
        container:   devcontainer               # optional — overrides source.container
        folder_path: rawfiles/dev               # optional — overrides source.folder_path; supports {storage_account}, {container}, {volume_name} tokens
        volume_name: dev-volume                 # optional — overrides source.volume_name (Unity Catalog volumes only)
      destination:
        dest_catalog:      dev_catalog
        dest_secret_scope: dev-kv-scope

    test:
      source:
        storage_account: testbronzestorage
        source_catalog:  test_catalog
        source_database: bronze
        source_secret_scope: test-kv-scope
      destination:
        dest_catalog:      test_catalog
        dest_secret_scope: test-kv-scope

    prod:
      source:
        storage_account: prodbronzestorage
        source_catalog:  prod_catalog
        source_database: bronze
        source_secret_scope: prod-kv-scope
      destination:
        dest_catalog:      prod_catalog
        dest_secret_scope: prod-kv-scope
```

Per-environment fields under `{env-name}.source` / `{env-name}.destination` **override** the corresponding top-level `source:` / `destination:` values. **Omitted fields fall back to the top-level values** — so an env profile only needs to specify what actually varies per environment. Fields that are truly identical across environments (e.g. `file_type`, `file_pattern`) can be left in the top-level blocks and need not be repeated in every profile.

> **REQUIRED at runtime:** `catalog` (from `dest_catalog`) and `database` (from `target_database`) must resolve to non-empty strings, either via the env profile's `destination:` block or via the top-level `destination:` block. The generated env cell emits a fail-fast `ValueError` if either is empty — this prevents a downstream `PARSE_SYNTAX_ERROR` on `MERGE INTO {catalog}.{database}.{table}` when one part is missing.

#### Detection modes

| `method` | Primary detection | Fallback |
|----------|-------------------|---------|
| `workspace_url` | `spark.conf.get("spark.databricks.workspaceUrl")` matched against `workspace_urls` map | `BION_ENV` cluster env var → `default_env` |
| `env_var` | `BION_ENV` cluster environment variable | `default_env` |
| `none` | (no runtime check) | Always uses `default_env` |

#### Pinned generation with `--env`

Pass `--env prod` to the `generate` command to bypass runtime detection and hardcode a specific environment in the generated notebook:

```bash
biontoolkit generate --model models/silver/CustomerConformed --env prod
```

This is useful for:
- Local development against a specific workspace
- Debugging what a prod notebook looks like
- Generating for a workspace whose URL hasn't been added to the map yet

When `--env` is used:
- `_bion_env = "prod"` is hardcoded (no URL-map lookup, no `BION_ENV` check)
- The `_ENV_CONFIG` dict is still embedded for reference
- Applicable only when `environments:` block is present in the model settings

#### `environments:` section fields

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `detection.method` | `workspace_url` \| `env_var` \| `none` | yes | How to detect the active environment at Databricks runtime. |
| `detection.default_env` | string | yes | Environment name to use when detection fails. Must be a key in the profiles below. |
| `detection.workspace_urls` | map | when `method: workspace_url` | Map of `{workspace-url: env-name}`. |
| `{env-name}.source.storage_account` | string | no | Azure storage account for this environment's source data. |
| `{env-name}.source.source_catalog` | string | no | Unity Catalog catalog for source data. |
| `{env-name}.source.source_database` | string | no | Database within `source_catalog`. |
| `{env-name}.source.source_secret_scope` | string | no | Databricks secret scope for source storage credentials. |
| `{env-name}.source.container` | string | no | Azure container override for this environment. When set, replaces the top-level `source.container` value at runtime. |
| `{env-name}.source.folder_path` | string | no | Folder/path override for this environment. Supports `{storage_account}`, `{container}`, `{volume_name}` runtime tokens that are substituted with the resolved environment values. |
| `{env-name}.source.volume_name` | string | no | Unity Catalog volume name override. When set, replaces the top-level `source.volume_name` value at runtime. |
| `{env-name}.destination.dest_catalog` | string | no¹ | Unity Catalog catalog for destination tables. Falls back to top-level `destination.dest_catalog`. |
| `{env-name}.destination.target_database` | string | no¹ | Database/schema within `dest_catalog` (e.g. `silver`). Falls back to top-level `destination.target_database`. |
| `{env-name}.destination.dest_secret_scope` | string | no | Databricks secret scope for destination storage credentials. |

¹ At least one of (env-profile value, top-level value) MUST be non-empty for `dest_catalog` and `target_database`. The env cell raises `ValueError` at runtime if either resolves empty.

#### Runtime placeholder substitution

When `environments:` is present, `TableRunConfig.source_path` is emitted as a template like `abfss://{container}@{storage_account}.dfs.core.windows.net/{folder_path}` (or `/Volumes/{source_catalog}/{source_database}/{volume_name}` for UC volumes). The env cell defines a `_env_subst(s)` helper that resolves these `{token}` placeholders against the env-resolved variables (`catalog`, `database`, `storage_account`, `container`, `folder_path`, `volume_name`, `source_catalog`, `source_database`, `source_secret_scope`, `dest_secret_scope`). Unknown tokens are left as-is (SafeDict / `__missing__`) so substitution never raises `KeyError`. The parameterized notebook routes `_cfg["source_path"]` and `_cfg["hwm_source_root"]` through `_env_subst` before use.

#### Generated resolution cell (example)

```python
# ── Environment Resolution ─────────────────────────────────────────────────
import os as _os

_workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
_url_to_env = {
    "adb-111111111111.1.azuredatabricks.net": "dev",
    "adb-999999999999.9.azuredatabricks.net": "prod",
}
_bion_env = _url_to_env.get(_workspace_url) or _os.environ.get("BION_ENV")
if not _bion_env:
    _bion_env = "dev"
    print(f"[BION] WARNING: workspace '{_workspace_url}' not in map, BION_ENV not set. Using 'dev'.")

_ENV_CONFIG = {
    "dev":  {"catalog": "dev_catalog", "storage_account": "devbronze", ...},
    "prod": {"catalog": "prod_catalog", "storage_account": "prodbronze", ...},
}
if _bion_env not in _ENV_CONFIG:
    raise ValueError(f"[BION] Unknown environment '{_bion_env}'. Available: {list(_ENV_CONFIG)}")

_env_cfg            = _ENV_CONFIG[_bion_env]
catalog             = _env_cfg["catalog"]
storage_account     = _env_cfg["storage_account"]
source_catalog      = _env_cfg["source_catalog"]
source_database     = _env_cfg["source_database"]
source_secret_scope = _env_cfg["source_secret_scope"]
dest_secret_scope   = _env_cfg["dest_secret_scope"]

print(f"[BION] Environment: {_bion_env} | catalog: {catalog} | workspace: {_workspace_url}")
```

Secret scope *names* are embedded in the notebook; secret *values* are always retrieved at runtime via `dbutils.secrets.get()` — never stored in generated code.

---

### Metadata JSON files (parameterized layout)

By default, when `artifact_layout: parameterized`, `RunAllNotebooks` has each table's full `TableRunConfig` JSON string **embedded inline** at generation time. This means every `biontoolkit generate` run regenerates `RunAllNotebooks` with fresh config.

Setting `processing.metadata_json_files_enabled: true` switches to a file-based approach:

- **Generation time:** A `{table_name}_config.json` file is written to a `table_configs/` subfolder inside the dependency folder for every table (alongside `RunAllNotebooks.py`, `Functions.py`, etc.).
- **Runtime:** `RunAllNotebooks` reads each JSON file at notebook execution time. The dependency folder path is resolved **at runtime** from the notebook's own workspace location — no absolute path config is needed and the same generated files work in dev, test, and prod without modification.
- **Benefit:** The JSON files can be inspected or modified on the workspace without regenerating notebooks. Only the config files need to be redeployed when table settings change.
- **Requirement:** Databricks Runtime 13.3 LTS or later (uses `open('/Workspace/...')` file API).
- **Path config:** Only `local_process_path` and `local_dependency_path` are needed. They must share a common prefix (after stripping any `<ENV:...>` token prefix) so the generator can derive the correct relative offset. `<ENV:...>` tokens are supported — they are stripped during computation and never appear in generated notebooks.
- **Portability:** The entire parent folder can be moved anywhere in the Databricks workspace. As long as the relative position of `RunAllNotebooks` and the table notebooks is unchanged, all paths resolve correctly.

```yaml
# settings.yaml
deployment:
  artifact_layout: parameterized
  local_process_path:    "<ENV:REPO_ROOT>/notebooks/silver/tables"   # local write path; <ENV:...> stripped for relative-path compute
  local_dependency_path: "<ENV:REPO_ROOT>/notebooks/silver/common"  # shares same <ENV:REPO_ROOT> prefix

processing:
  metadata_json_files_enabled: true   # default: false (inline embedding)
```

**What gets baked into `RunAllNotebooks`** (no local paths, fully portable):
```python
_nb_path     = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
dep_path     = _nb_path.rsplit("/", 1)[0]   # runtime workspace path of this notebook's folder
_proc_offset = "../tables"                  # relative offset — baked at generation time
process_path = dep_path + "/" + _proc_offset
```

When `false` (default), behavior is unchanged — all config is embedded inline in `RunAllNotebooks` and no `.json` files are written.

---

## Table YAML — Table Settings

Each file in `tables/` describes one Delta table. The filename stem (e.g. `Customer.yaml`) is used as the table name unless a `table_name:` or `name:` key is present.

### Minimal example

```yaml
table_name: Customer
columns:
  - name: CustomerID
    data_type: INTEGER
    is_source_key: true
  - name: CustomerName
    data_type: STRING
  - name: Email
    data_type: STRING
```

### Full example

```yaml
table_name: Customer
is_active: true

settings:
  source_file_pattern: "Customer_*"
  table_query: null
  include_in_workflow: true
  skip_on_build: false
  infer_missing_records: false
  view_only: false
  suppress_date_fk: false
  beginning_cell: |
    print("Starting Customer load")
  pre_query_cell: null
  end_cell: |
    print("Customer load complete")
  expectations:
    - name: email_not_null
      expression: "Email IS NOT NULL"
      action: write_and_audit
    - name: valid_status
      expression: "Status IN ('Active', 'Inactive')"
      action: drop

columns:
  - name: CustomerID
    data_type: INTEGER
    is_source_key: true
  - name: CustomerSK
    data_type: BIGINT
    is_surrogate_key: true
  - name: CustomerName
    data_type: STRING
  - name: Email
    data_type: STRING
  - name: ModifiedDate
    data_type: TIMESTAMP
    is_high_watermark: true
  - name: RegionID
    data_type: INTEGER
    foreign_table: Region
  - name: Revenue
    data_type: DECIMAL
    precision: 18
    scale: 4
    expression: "CAST(<value> AS DECIMAL(18,4))"
  - name: LoadDate
    data_type: DATE
    is_partition_by: true
  - name: CustomerName_upper
    data_type: STRING
    expression: "UPPER(<value>)"
    is_active: false
```

### `settings` block (table-level overrides)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source_file_pattern` | string \| null | `null` | Overrides the model-level `file_pattern` for this table only. Same token support (`<tablename>` etc.). |
| `table_query` | string \| null | `null` | Custom SparkSQL `SELECT` statement used as the source for this table. Required for Gold and Tier-2 Silver tables that join across sources. |
| `include_in_workflow` | bool | `true` | When `false`, this table is excluded from `RunAllNotebooks` execution. |
| `skip_on_build` | bool | `false` | Skip this table entirely during `generate` and `deploy` (useful for temporarily disabling a table without deleting its YAML). |
| `is_date_table` | bool | `false` | Mark this table as the Date Dimension; suppresses auto-generation of Date FK references pointing back to it. |
| `infer_missing_records` | bool \| null | `null` | Override the model-level `infer_missing_records_enabled` flag for this table. |
| `dedup_source_enabled` | bool \| null | `null` | Override the model-level `dedup_source_enabled` flag for this table only. |
| `dedup_source_recency_column` | string \| null | `null` | Override the model-level `dedup_source_recency_column` for this table only. |
| `view_only` | bool | `false` | Generate a `CREATE OR REPLACE VIEW` for this table instead of a managed Delta table. |
| `suppress_date_fk` | bool | `false` | Do not auto-generate Date Dimension foreign key references for this table even if date columns are present. |
| `beginning_cell` | string \| null | `null` | Python code string injected at the start of this table's notebook (runs before source read). Enabled when `processing.beginning_cells_enabled: true`. |
| `pre_query_cell` | string \| null | `null` | Python code string injected just before the MERGE statement. |
| `end_cell` | string \| null | `null` | Python code string injected at the end of the notebook, after MERGE and statistics. |

### `expectations` — Data Quality Rules

Defined per table under `settings.expectations`. Requires `processing.expectations_enabled: true` on the model.

```yaml
settings:
  expectations:
    - name: revenue_positive
      expression: "Revenue > 0"
      action: write_and_audit
```

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `name` | string | yes | Human-readable rule name. Also used as the audit column name suffix. |
| `expression` | string | yes | SparkSQL boolean expression evaluated per row. `true` = row passes. |
| `action` | string | yes | What to do when the expression is `false` — see table below. |

**Expectation actions:**

Behaviour differs between the **notebook path** and the **DLT path** (`streaming_enabled: true` or `artifact_type: dlt`).

| Action | Notebook path | DLT path |
|--------|--------------|----------|
| `write_and_audit` | Write row to target table AND write to `{Table}_Audit_DataQualityViolations`. | Write row to primary table (`@dp.expect`). When `dlt_quarantine_enabled: true`, also write to companion `quarantine_{Table}` if expression failed. |
| `audit_only` | Write row to audit table only — **exclude** from target table. | Exclude row from primary table (`@dp.expect_or_drop`). When `dlt_quarantine_enabled: true`, write to companion `quarantine_{Table}`. |
| `drop` | Silently drop the violating row — no audit trail. | Silently drop the violating row (`@dp.expect_or_drop`). **Not** written to quarantine even when `dlt_quarantine_enabled: true`. |
| `fail` | Raise a `RuntimeError` and abort the notebook run if any violating rows exist. | Pipeline halts (`@dp.expect_or_fail`). |
| `write_only` | Write the row to the target table — no audit. | Write the row unconditionally — no decorator emitted. |

The audit table `{catalog}.{database}.{TableName}_Audit_DataQualityViolations` is created automatically by `UpdateSchema` when `expectations_enabled: true` and the table has at least one expectation.

---

## Column Reference

Each entry in `columns:` describes one column in the target Delta table.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `name` | string | **required** | Column name in the target table. |
| `data_type` | string | `STRING` | Spark/Delta data type: `STRING`, `INTEGER`, `BIGINT`, `DOUBLE`, `DECIMAL`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `BINARY`. |
| `precision` | int \| null | `null` | For `DECIMAL` — total significant digits (e.g. `18`). |
| `scale` | int \| null | `null` | For `DECIMAL` — digits after the decimal point (e.g. `4`). |
| `size` | int \| null | `null` | For `VARCHAR` / `CHAR` — maximum character length. |
| `is_source_key` | bool | `false` | Part of the business / natural key. At least one column per table must be `true`. Used in the MERGE ON condition (unless `merge_on_surrogate_key: true`). |
| `is_surrogate_key` | bool | `false` | This column holds the SHA2 hash surrogate key computed from the source key columns. At most one per table. When `processing.surrogate_keys_enabled: true`, you do not need to declare this column (or its `_Id` integer companion) yourself — see "Column auto-synthesis" under Surrogate / hash keys above. |
| `is_high_watermark` | bool | `false` | This column drives incremental filtering when `use_high_watermark: true`. At most one per table. |
| `foreign_table` | string \| null | `null` | Name of a foreign table whose surrogate key this column references (e.g. `Region`). Used to generate FK surrogate-key lookup joins. |
| `is_type4` | bool | `false` | This column participates in SCD Type 4 history tracking. |
| `expression` | string \| null | `null` | SparkSQL transform applied to this column at load time. The placeholder `<value>` is replaced with the source column reference. Example: `"UPPER(<value>)"`, `"CAST(<value> AS DECIMAL(18,4))"`. |
| `alternate_key_group` | string | `""` | Alternate Key group identifier for multi-source integration scenarios. |
| `is_partition_by` | bool | `false` | Include this column in `PARTITION BY` on the `CREATE TABLE` DDL. |
| `is_zorder_by` | bool | `false` | Include this column in `OPTIMIZE … ZORDER BY` at the end of each notebook run. |
| `is_active` | bool | `true` | When `false`, the column is retained in YAML but excluded from all generated code. |

---

## UpdateSchema — Shared Notebook Behaviour

`UpdateSchema.py` is a shared notebook generated alongside the table notebooks. It can be run once per environment to create the schema and all target tables up front; this is optional, because table notebooks create their own table and back-fill missing framework columns (nothing runs `UpdateSchema` automatically).

- Emits `CREATE CATALOG IF NOT EXISTS` and `CREATE SCHEMA IF NOT EXISTS` unless `dest_catalog_suppress_create` / `dest_database_suppress_create` are set.
- Emits `CREATE TABLE IF NOT EXISTS` for each active table when `include_create_statements: true`.
- Emits `CREATE TABLE IF NOT EXISTS {TableName}_Audit_DataQualityViolations` for each table that has expectations defined, when `expectations_enabled: true`.
- Creates auxiliary tables when their associated feature flags are enabled:
  - `Audit_RuntimeTableLog` — created when `table_statistics_enabled: true`
  - `HighWatermark` — created when `use_high_watermark: true`

**`artifact_layout` options:**

| Value | Behavior |
|-------|----------|
| `per_table` (default) | One `.py` notebook per table — `Customer.py`, `Address.py`, etc. |
| `consolidated` | One `.py` notebook for the whole model — `CustomerConformed.py` containing all tables' code as separate inline sections. Code is still hardcoded per table — **not** metadata-driven. |
| `parameterized` | A single `{ModelName}_ProcessTable.py` whose ETL logic is shared by all tables. Table-specific config is passed as a JSON widget argument at runtime. `RunAllNotebooks` calls it once per table in parallel. **This is the metadata-driven layout.** |

### `settings.yaml` — Gold model example

```yaml
model_name: SalesDW
zone: gold

settings:
  source:
    cloud_provider: azure
    source_type: table
    source_catalog: dev_catalog
    source_database: silver

  destination:
    zone: gold
    target_database: gold
    dest_catalog: dev_catalog
    managed_tables: false
    dest_cloud_provider: azure
    dest_storage_account: "<ENV:GOLD_STORAGE_ACCOUNT>"
    dest_container: gold
    dest_folder_path: salesdw

  processing:
    surrogate_keys_enabled: true
    merge_on_surrogate_key: true  # use SK column in MERGE ON clause (required for dimensional pattern)
    hash_function: "UNHEX(SHA2(<stringvalue>, 256))"
    hash_multi_value_delimiter: "|"
    int_key_identifier: "_Id"
    infer_missing_records_enabled: true
    table_statistics_enabled: true
    include_create_statements: true
    change_hash_enabled: true
    optimize_enabled: true
    vacuum_enabled: true
    ansi_mode: true
    add_grave_accent: true

  deployment:
    local_deployment_enabled: true
    local_process_path:    "<ENV:REPO_ROOT>/notebooks/gold/tables"   # local write path only
    local_dependency_path: "<ENV:REPO_ROOT>/notebooks/gold/common"  # must share a common prefix with local_process_path
```

---

### Column flag reference

| Flag | Meaning |
|------|---------|
| `is_source_key` | Natural/business key — used in MERGE ON clause |
| `is_surrogate_key` | SHA2 binary surrogate key; set `merge_on_surrogate_key: true` in processing to use it in the MERGE clause. With `surrogate_keys_enabled: true`, this column (and its `_Id` companion) is auto-synthesized if not declared — see "Column auto-synthesis" earlier in this document. |
| `is_high_watermark` | Timestamp used for incremental extraction |
| `is_type4` | SCD Type 4 — full row stored in history table on change |
| `foreign_table: <TableName>` | References a surrogate key in another dimension — set on the column, not a bool flag |
| `is_active` | Include column in generated code (false = excluded) |

### Supported data types

`STRING`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `FLOAT`, `DOUBLE`,
`DECIMAL(p,s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `BINARY`, `ARRAY<T>`, `MAP<K,V>`

---

## Workspace Configuration (`workspace.yaml`)

Optionally place at the project root. Discovered by walking up from the current directory.

```yaml
name: MyWorkspace
default_catalog: dev_catalog
cloud_provider: azure
global_settings:
  drawio_executable_path: "C:/Program Files/draw.io/draw.io.exe"
```

---

## Checking the Installed Version

Two equivalent ways to read the currently installed `biontoolkit` version:

```bash
biontoolkit version       # prints: bion-toolkit 0.5.11
biontoolkit --version     # prints: bion-toolkit, version 0.5.11
```

From Python (e.g. in a notebook or script):

```python
from biontoolkit import __version__
print(__version__)        # e.g. "0.3.0"
```

Use the version string to confirm which feature set is available before generating or deploying models.

---

## Metadata API — `biontoolkit.metadata`

The `biontoolkit.metadata` package provides a Python API for loading and validating YAML model definitions, and for importing table schemas from external sources.

### Loading and resolving YAML

```python
from biontoolkit.metadata import load_global, load_model, resolve

# Load the optional workspace-level global settings
global_cfg = load_global("workspace.yaml")          # returns GlobalConfig | None

# Load a model from a directory (settings.yaml + tables/*.yaml)
# or from a single-file YAML
model_cfg, table_cfgs = load_model("models/silver/CustomerConformed")

# Resolve the effective settings for one table
resolved = resolve(global_cfg, model_cfg, table_cfgs[0])  # returns ResolvedSettings
```

### Metadata importers — building models from external sources

Importers read schema metadata from an external source (CSV file, Parquet file) and return the same `(ModelConfig, list[TableConfig])` tuple that `load_model()` returns. The result can be passed directly to the resolver and generators.

```python
from biontoolkit.metadata.importer import CsvMetadataImporter, ParquetMetadataImporter
from biontoolkit.metadata.models.model_settings import MedallionLayer

# Import schema from a CSV file
importer = CsvMetadataImporter(
    path="data/Customer.csv",
    model_name="CustomerConformed",
    layer=MedallionLayer.SILVER,
    delimiter=",",
    has_header=True,
)
model_cfg, table_cfgs = importer.import_model()
# All columns default to STRING; first column is source_key=True

# Import schema from a Parquet file (requires: pip install biontoolkit[parquet])
importer = ParquetMetadataImporter(
    path="data/Customer.parquet",
    model_name="CustomerConformed",
)
model_cfg, table_cfgs = importer.import_model()
# Column types inferred from Parquet schema (INT, BIGINT, DOUBLE, DECIMAL, etc.)
```

#### `CsvMetadataImporter` constructor

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `Path \| str` | required | Path to the CSV file. |
| `model_name` | `str` | required | Name for the generated `ModelConfig`. |
| `table_name` | `str \| None` | `None` (CSV stem) | Name for the generated `TableConfig`. |
| `layer` | `MedallionLayer` | `SILVER` | Medallion zone label. |
| `delimiter` | `str` | `","` | Column separator. |
| `has_header` | `bool` | `True` | First row contains column names. When `False`, columns are named `col_1`, `col_2`, etc. |

#### `ParquetMetadataImporter` constructor

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `Path \| str` | required | Path to the Parquet file. |
| `model_name` | `str` | required | Name for the generated `ModelConfig`. |
| `table_name` | `str \| None` | `None` (file stem) | Name for the generated `TableConfig`. |
| `layer` | `MedallionLayer` | `SILVER` | Medallion zone label. |

**Type mapping (Parquet → framework):**

| Parquet / Arrow type | Framework `data_type` |
|---|---|
| `int8`, `int16`, `int32` | `INT` |
| `int64` | `BIGINT` |
| `float32` | `FLOAT` |
| `float64` | `DOUBLE` |
| `bool` | `BOOLEAN` |
| `date32`, `date64` | `DATE` |
| `timestamp(...)` | `TIMESTAMP` |
| `binary`, `large_binary` | `BINARY` |
| `decimal128(p, s)` | `DECIMAL` (with `precision` and `scale`) |
| `string`, `large_string`, `utf8` | `STRING` |
| anything else | `STRING` (safe fallback) |

---

## Generator Architecture

All notebook generation is handled by a single unified class — `MedallionNotebookGenerator`. There are no separate Silver or Gold generator classes. The `zone` field on a model (`silver`, `gold`, etc.) is a **descriptive label only** — it does not select a different code path.

Both the `generate` and `deploy` commands use identical layout-routing logic:

| `artifact_layout` / mode | Generator called | Output |
|--------------------------|-----------------|--------|
| `per_table` (default) | `generator.generate(ctx)` per table | One `.py` per table |
| `consolidated` | `generator.generate_model(contexts)` | One `.py` for the whole model |
| `parameterized` | `generator.generate_parameterized_notebook(contexts)` | `{ModelName}_ProcessTable.py` |
| DLT (`streaming_enabled` or `artifact_type: dlt`), `dlt_layout: consolidated` (default) | `DLTGenerator.generate_pipeline(contexts)` | One DLT pipeline file (`{ModelName}_pipeline.py`) |
| DLT (`streaming_enabled` or `artifact_type: dlt`), `dlt_layout: per_table` | `DLTGenerator.generate_pipeline_per_table(contexts)` | One self-contained DLT file per active table (`{TableName}.py`) |

**`TableRunConfig`** — the metadata bridge for parameterized layout.
At generation time, `TableRunConfig.from_context()` serializes all per-table
configuration (catalog, database, columns, merge keys, feature flags, DDL,
expectations, custom cells) to a JSON string. This string is embedded in
`RunAllNotebooks` and passed to `{ModelName}_ProcessTable.py` at runtime via
`dbutils.widgets`. The notebook never changes between runs — only the JSON
config does. The `to_json()` / `from_json()` round-trip is lossless; `from_json()`
raises a descriptive `ValueError` on a corrupted payload.

Feature activation is entirely flag-driven from YAML settings:

| Feature | Activated by |
|---------|-------------|
| Surrogate key hashing | `surrogate_keys_enabled: true` + `is_surrogate_key` column flag |
| SK-based MERGE | `merge_on_surrogate_key: true` in `processing` |
| Source-key MERGE | default (omit `merge_on_surrogate_key`, or set `false`) |
| SCD Type 4 | `is_type4` column flag |
| Infer missing records | `infer_missing_records_enabled: true` |
| Auto Loader | `use_auto_loader: true` in `source` |
| Effective dates | `effective_dates_enabled: true` |

Notebook generation is done programmatically — Jinja2 / template files are not used.

---

## UpdateSchema — Shared Notebook Behaviour

The `UpdateSchema` shared notebook (generated once per model into the dependency folder) handles full schema bootstrapping in order:

1. Sets `catalog` and `database` variables from model settings
2. `CREATE SCHEMA IF NOT EXISTS {catalog}.{database}` — ensures the Unity Catalog schema exists
3. Creates auxiliary/metadata tables (see below) if they do not yet exist
4. `CREATE TABLE IF NOT EXISTS` for every active model table — adds new columns with `ALTER TABLE … ADD COLUMNS` if the table already exists

All creation steps are skipped when `dest_database_suppress_create: true` is set in `destination` settings (useful when the catalog/schema is managed externally).

### Auxiliary tables created by UpdateSchema

| Table | Columns | Created when |
|-------|---------|-------------|
| `Audit_RuntimeTableLog` | `TableName STRING NOT NULL`, `RowCount BIGINT`, `RunTimestamp TIMESTAMP` | `table_statistics_enabled: true` (default) |
| `HighWatermark` | `TableName STRING NOT NULL`, `ColumnName STRING NOT NULL`, `MaxValue STRING` | `use_high_watermark: true` in `source` |

Note: `UpdateSchema`'s `CREATE TABLE IF NOT EXISTS` DDL includes the same framework columns as the table notebook's own DDL (`Audit_CreatedDateTime`, `Audit_ModifiedDateTime`; `Audit_EffectiveDate`/`Audit_ExpiredDate`/`Audit_RowIsCurrent` with `effective_dates_enabled`; `Audit_IsDeleted` with the soft-delete flag; `ChangeHash` with `change_hash_enabled` on a table with non-key business columns), and it backfills any of them missing from an existing table via `ALTER TABLE … ADD COLUMNS`. The Date Dimension (`is_date_table: true`) gets none of them, in its notebook or in `UpdateSchema`. These are system-injected and do not require any setting. `UpdateSchema` is a standalone notebook: RunAllNotebooks and the bundle job do not run it, and running it is optional, because table notebooks add missing framework columns themselves (see [Upgrade notes](#upgrade-notes)).

All auxiliary tables are Delta tables (`USING delta`) created in the same catalog/schema as the model tables. Running UpdateSchema is safe to re-run at any time — all statements use `IF NOT EXISTS`.

---

## Consumer Customization

BionToolkit provides three mechanisms for consuming projects to apply consistent, automated customizations to generated output at every build or deploy without losing changes.

---

### Model-level cell injection — `model_beginning_cell` / `model_end_cell`

Python code declared in `settings.yaml` that is injected as a notebook cell into **every table notebook generated for the model**. Unlike `table.settings.beginning_cell` (per-table, requires `beginning_cells_enabled: true`), the model-level cells apply globally and are always emitted when set.

| Key (in `settings.yaml`, top-level) | Type | Default | Description |
|--------------------------------------|------|---------|-------------|
| `model_beginning_cell` | string \| null | `null` | Python injected after env-resolution / settings-dump, before any per-table `beginning_cell`. Applies to every table in the model. |
| `model_end_cell` | string \| null | `null` | Python injected after any per-table `end_cell`, before VACUUM/OPTIMIZE. Applies to every table in the model. |

Works with `per_table`, `consolidated`, and `parameterized` artifact layouts.

**Example — add a model-wide import and tag every run:**
```yaml
# settings.yaml
model_beginning_cell: |
  import my_project.utils as _utils
  _utils.log_run_start(table_name, catalog, database)

model_end_cell: |
  _utils.log_run_end(table_name, catalog, database)
```

---

### Custom notebooks — `custom_notebooks`

A list of hand-authored `.py` Databricks notebook files (paths relative to the model directory) that are copied into the output alongside generated notebooks at every `generate` or `deploy`. Use this for custom orchestration notebooks, reference notebooks, or any supplementary code the toolkit does not generate automatically.

```yaml
# settings.yaml
custom_notebooks:
  - custom/InitSecrets.py
  - custom/PostLoadAudit.py
```

Each file is read as-is and written to the tables output folder. The files are picked up by all deployers (local and API) transparently.

> **Tip:** Keep custom notebooks under a `custom/` subfolder of the model directory so they are clearly distinguished from generated content.

---

### Post-generate hook — `post_generate.py`

For full flexibility, place a `post_generate.py` file anywhere in the project directory tree (the toolkit walks upward from the model directory). If found, it is called automatically after all files are written.

**Interface (your script must define this function):**
```python
def post_generate(results: list[str], output_dir: str) -> None:
    """
    Called after all notebooks have been written to disk.

    Args:
        results:    Absolute paths of every file written in this generate run.
        output_dir: Absolute path to the root output directory.
    """
    ...
```

**Example — patch a specific generated file:**
```python
def post_generate(results, output_dir):
    from pathlib import Path
    target = Path(output_dir) / "tables" / "Order.py"
    if target.exists():
        text = target.read_text()
        text = text.replace("OLD_VALUE", "NEW_VALUE")
        target.write_text(text)
```

The hook only runs when `--output <dir>` is specified (i.e., when files are actually written). Errors in the hook are reported as warnings and do not abort the run.

> **Note:** `post_generate.py` discovery uses a walk-up search identical to `workspace.yaml`. A file at the repo root will be found regardless of which model is being generated.
