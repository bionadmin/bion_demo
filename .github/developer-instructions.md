# biontoolkit — Developer Guide

> **Audience:** Human developers working with a Copilot-enabled repository that uses biontoolkit.
> The companion `copilot-instructions.md` (in this same `.github/` folder) is read automatically
> by the AI agent. This file is for **you** — to help you understand what the agent can do and
> how to direct it effectively.

---

## What is biontoolkit?

biontoolkit is a metadata-driven notebook generator for Databricks Medallion architecture.
You describe your data models in YAML, and biontoolkit generates production-ready PySpark
notebooks for Silver and Gold zones — including MERGE logic, schema evolution, data quality
expectations, surrogate keys, effective dating (SCD2), and more.

The AI agent in this repository (GitHub Copilot, or Claude Code) has access to the full
biontoolkit reference via the `biontoolkit` Agent Skill under `.claude/skills/` — it loads
automatically when relevant. You don't need to explain the toolkit to it — just tell it
what you want to build.

---

## Getting Started — First-Time Setup

### 1. Install biontoolkit

`biontoolkit` depends on a separate small package, `bion-etl-kernel`. Install from the private
index (recommended — use `--index-url`, never `--extra-index-url`, or pip may resolve either
package from a same-named public PyPI package instead of this one):

```bash
pip install biontoolkit --index-url https://raw.githubusercontent.com/bionadmin/biontoolkitdistrib/main/simple/
```

`bion-etl-kernel` resolves automatically from the same index — no separate install step needed.

If you don't have index access, a direct Git install still works, but needs two specs in one
invocation (the kernel lives in a subdirectory of that repo, not the root, so a plain
`pip install git+https://github.com/bionadmin/BionToolkit.git` no longer resolves it alone):

```bash
pip install \
  "bion-etl-kernel @ git+https://github.com/bionadmin/BionToolkit.git#subdirectory=packages/bion-etl-kernel" \
  "biontoolkit @ git+https://github.com/bionadmin/BionToolkit.git"
```

Then update the Copilot instructions to match the installed version:

```bash
biontoolkit update-docs
```

This writes `.github/copilot-instructions.md` and `.github/developer-instructions.md`
(this file) into your project, so the agent always uses the correct reference for the
version you have installed.

### 2. Create your first model

Ask the agent:

> "Create a Silver model called CustomerConformed that loads from Azure Data Lake Storage
> using Unity Catalog volumes. The source tables are Customer and Address."

The agent will scaffold the `settings.yaml` and table YAML files. Review and adjust the
generated paths before running `biontoolkit generate`.

### 3. Generate and review

```bash
biontoolkit generate --model models/silver/CustomerConformed --dry-run
```

Use `--dry-run` to review the generated notebooks in the terminal before writing them to disk.

### 4. Deploy

```bash
biontoolkit deploy --model models/silver/CustomerConformed --target api
```

Requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables.

---

## How to Prompt the Agent Effectively

The agent knows the full YAML schema and all biontoolkit features. The most effective prompts
are **declarative** — describe the outcome you want, not the YAML syntax.

### Pattern 1 — Describe the source and destination

> "The Account table comes from Salesforce via a file drop in our Azure Data Lake container
> `raw-data`, folder `salesforce/account`. It should land in the `silver` catalog, `crm`
> database. Use Unity Catalog volumes."

The agent will produce a complete `settings.yaml` + `Account.yaml`.

### Pattern 2 — Add a feature to an existing table

> "Enable change hashing on the Customer table so we only MERGE rows that have actually changed."

The agent will add `change_hash_enabled: true` to the table YAML and explain the impact on
the generated MERGE cell.

### Pattern 3 — Enable effective dating (SCD2)

> "The Product table needs full SCD2 — keep history with effective and expiry dates,
> and mark the current row with Audit_RowIsCurrent."

The agent will set `effective_dates_enabled: true` and configure the related audit columns.

> SCD2 needs non-NULL merge (business) key values: the SCD2 merges match keys with `=`, so
> a row with a NULL key never matches its own history. Fix or filter NULL keys upstream.

### Pattern 4 — Add data quality rules

> "Add an expectation to the Order table that fails the run if OrderTotal is negative."

```
# Agent will add to Order.yaml:
expectations:
  - name: order_total_non_negative
    expression: "OrderTotal >= 0"
    action: fail
```

### Pattern 5 — Troubleshoot a generated notebook

Paste the error message directly:

> "The notebook is throwing: AnalysisException: Cannot resolve column Audit_ModifiedDateTime.
> Here is the MERGE cell: [paste cell]. What's wrong?"

The agent understands the generated cell structure and will diagnose schema evolution issues,
MERGE predicate problems, and Databricks Serverless compatibility constraints.

### Pattern 6 — Gold model from a Silver model

> "Create a Gold model called SalesDW that reads from the CustomerConformed Silver model.
> I need DimCustomer and FactSales. DimCustomer should merge on SK_Customer."

The agent will generate a Gold `settings.yaml` with `source_type: table` referencing
the Silver catalog/database.

### Pattern 7 — Understand a generated notebook

> "Explain what the RunAllNotebooks notebook does and in what order it runs the tables."

---

## Common Workflows

### Adding a new source table

1. Ask the agent: *"Add a new table called `Opportunity` to the CustomerConformed Silver model. It has columns: OpportunityId (merge key), AccountId, Name, StageName, CloseDate, Amount, CreatedDate, ModifiedDate."*
2. The agent creates `tables/Opportunity.yaml`.
3. Run `biontoolkit validate --model models/silver/CustomerConformed` to check for errors.
4. Run `biontoolkit generate --model models/silver/CustomerConformed` to produce the notebook.
5. Run `biontoolkit deploy --model models/silver/CustomerConformed --tables Opportunity --target api` to deploy only the new table.

### Evolving an existing table's schema

When new columns arrive in the source:

1. Add the columns to the table YAML: *"Add a column `Rating` (STRING) to Account.yaml"*
2. Regenerate: `biontoolkit generate --model models/silver/CustomerConformed --tables Account`
3. Redeploy: `biontoolkit deploy --model models/silver/CustomerConformed --tables Account --target api`
4. Optional: run `UpdateSchema` in Databricks to `ALTER TABLE … ADD COLUMNS` for the new column up front.

> The generated process notebooks perform serverless-safe schema evolution automatically
> before each MERGE, so existing data loads are safe without running `UpdateSchema`; it only
> brings the DDL up to date ahead of the next load.

### Promoting from dev to prod

Use the `environments` block in `settings.yaml` to define catalog/path overrides per environment:

> "Add dev, test, and prod environments to CustomerConformed. Dev uses `dev_catalog`, prod
> uses `prod_catalog`. The Databricks workspace URLs differ per environment."

---

## Customizing Generated Output

biontoolkit regenerates notebooks from YAML every time you run `generate` or `deploy`. These
three mechanisms let you apply consistent customizations at every build without losing changes.

### 1. Model-level cell injection (`model_beginning_cell` / `model_end_cell`)

Add Python code to `settings.yaml` that is injected into **every table notebook** in the model — no per-table repetition needed. Unlike `table.settings.beginning_cell` (which requires `beginning_cells_enabled: true` and must be set per table), these fields are always emitted when present.

```yaml
# settings.yaml
model_beginning_cell: |
  import my_project.utils as _utils
  _utils.log_run_start(table_name, catalog, database)

model_end_cell: |
  _utils.log_run_end(table_name, catalog, database)
```

- `model_beginning_cell` runs before the per-table `beginning_cell`
- `model_end_cell` runs after the per-table `end_cell`, before VACUUM/OPTIMIZE
- Works with `per_table`, `consolidated`, and `parameterized` artifact layouts

### 2. Custom notebooks (`custom_notebooks`)

Declare hand-authored `.py` Databricks notebook files to copy into the output at every generate/deploy. Paths are relative to the model directory.

```yaml
# settings.yaml
custom_notebooks:
  - custom/InitSecrets.py
  - custom/PostLoadAudit.py
```

The files are read as-is and written to the tables output folder alongside generated notebooks. All deployers pick them up transparently. Keep your hand-authored notebooks in a `custom/` subfolder so they are easy to distinguish from generated output.

### 3. Post-generate hook (`post_generate.py`)

Place a `post_generate.py` file anywhere in the project tree (biontoolkit walks up from the model directory to find it, just like `workspace.yaml`). It is called automatically after all files are written.

```python
# post_generate.py — place at the repo root (or any parent of the model dir)

def post_generate(results: list[str], output_dir: str) -> None:
    """
    Called after all notebooks have been written.

    Args:
        results:    Absolute paths of every file written in this run.
        output_dir: Absolute path to the root output directory.
    """
    from pathlib import Path
    # Example: patch a specific generated file
    target = Path(output_dir) / "tables" / "Order.py"
    if target.exists():
        text = target.read_text()
        text = text.replace("OLD_PATTERN", "NEW_PATTERN")
        target.write_text(text)
```

The hook only fires when `--output <dir>` is used (i.e., when files are actually written). Errors are reported as warnings and do not abort the run. A closer `post_generate.py` (nearer the model directory) takes precedence over one further up the tree.

### When to use which

| Need | Mechanism |
|------|-----------|
| Add the same import or variable to every notebook | `model_beginning_cell` |
| Run cleanup or audit code after every table notebook | `model_end_cell` |
| Ship extra orchestration or reference notebooks with the model | `custom_notebooks` |
| Patch or transform generated files, or add fully dynamic notebooks | `post_generate.py` |



The agent will configure the `environments:` block and the runtime detection logic.

### Importing an existing Salesforce schema

```bash
biontoolkit import salesforce --object Account --output models/salesforce/tables/
```

Or ask the agent:

> "Import the Salesforce Account object schema and create a Silver table YAML for it."

---

## Effective Prompting — Tips and Anti-Patterns

### Do

- **Give concrete column names and types.** The agent generates better DDL and MERGE logic
  when it knows the actual schema rather than placeholders.
- **State your Databricks environment up front** (Azure vs AWS, Unity Catalog vs legacy Hive,
  Serverless vs classic compute). This affects the generated code paths.
- **Ask the agent to validate before generating.** *"Validate the model first, then generate."*
- **Paste error messages verbatim.** The agent recognises Databricks and Delta error codes.
- **Ask for a dry run first.** *"Generate with --dry-run and show me the MERGE cell."*

### Avoid

- **Don't write YAML by hand for complex features.** Surrogate keys, effective dating, and
  expectations have interdependencies that are easy to misconfigure. Ask the agent to
  generate them and explain the choices.
- **Don't re-explain biontoolkit concepts.** The agent already has the full reference.
  Just describe your data requirements.
- **Don't paste entire notebooks when troubleshooting.** Paste only the failing cell and
  the error message. The agent knows the cell structure from its instructions.
- **Don't ask the agent to edit generated notebooks directly.** Generated notebooks are
  overwritten on the next `generate` or `deploy`. Always change the source YAML and
  regenerate.

---

## Key Concepts to Understand

| Concept | What it means | How to ask for it |
|---------|---------------|-------------------|
| **Merge key** | Column(s) used in the MERGE ON predicate | *"Use OrderId as the merge key"* |
| **Surrogate key** | Hash-based SK column added by the pipeline | *"Add a surrogate key SK_Customer hashed from CustomerId and SourceSystem"* |
| **Change hash** | Only MERGEs rows where source differs from target | *"Only update rows that have actually changed"* |
| **Effective dating** | SCD2 — keeps full row history with start/end dates | *"Enable SCD2 history on Product"* |
| **Not-in-source deletes** | Soft or hard deletes rows removed from the source | *"Delete rows in target that are no longer in the source"* |
| **Source deduplication** | Pre-MERGE dedup to handle duplicate source rows — picks most-recent by a specified column | *"Enable source dedup on Account using SystemModstamp as the recency column"* |
| **Expectations** | Row-level data quality rules with configurable actions | *"Fail the run if CustomerId is null"* |
| **Parameterized layout** | Single `ProcessTable` notebook driven by JSON widget config | *"Use the parameterized layout for this model"* |
| **UpdateSchema** | Optional shared notebook that CREATE/ALTER TABLEs up front; nothing runs it automatically, and table notebooks create their own table and back-fill missing framework columns anyway | *"Do I need to run UpdateSchema before the first load?"* (no) |
| **RunAllNotebooks** | Orchestration notebook that runs all tables in parallel | *"How does RunAllNotebooks decide the execution order?"* |

---

## Keeping Documentation Current

After upgrading biontoolkit, run:

```bash
biontoolkit update-docs
```

This updates `.github/copilot-instructions.md`, `CLAUDE.md`, `.github/developer-instructions.md`
(this file), and the `biontoolkit` Agent Skill under `.claude/skills/` to match the
installed version. Commit all of it so the whole team benefits.

---

## Getting Help

- **Ask the agent:** It has the full biontoolkit reference and can explain any YAML field,
  generated cell, or runtime behaviour.
- **Validate first:** `biontoolkit validate --model <path>` catches most YAML errors before
  generation.
- **Check the version:** `biontoolkit version` — include this when reporting issues.
