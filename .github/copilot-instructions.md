# biontoolkit — Copilot Instructions

This project uses the `biontoolkit` CLI to generate and deploy Medallion-architecture
ETL notebooks for Databricks (Azure and AWS). All data models are defined as YAML files.

## Installation

`biontoolkit` depends on a separate small package, `bion-etl-kernel`. A plain
`pip install git+https://github.com/bionadmin/BionToolkit.git` no longer resolves it on its
own — the kernel lives in a subdirectory of that repo, not the root — so use one of the two
options below instead.

### Private index (recommended)

```bash
pip install biontoolkit --index-url https://raw.githubusercontent.com/bionadmin/biontoolkitdistrib/main/simple/
```

`bion-etl-kernel` resolves automatically from the same index. **Use `--index-url`, not
`--extra-index-url`** — see Key Constraints below.

### Direct Git install (fallback, no index required)

Two specs in one invocation — one per subdirectory — so both packages resolve together:

```bash
pip install \
  "bion-etl-kernel @ git+https://github.com/bionadmin/BionToolkit.git#subdirectory=packages/bion-etl-kernel" \
  "biontoolkit @ git+https://github.com/bionadmin/BionToolkit.git"
```

### With a virtual environment (recommended for isolation)

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install biontoolkit --index-url https://raw.githubusercontent.com/bionadmin/biontoolkitdistrib/main/simple/
```

### With `uv` (faster installs)

```bash
uv pip install biontoolkit --index-url https://raw.githubusercontent.com/bionadmin/biontoolkitdistrib/main/simple/
```

> **Windows note:** On Windows with a Windows Store Python, `pip install` may fail with "Failed to canonicalize script path". Use `uv pip install` instead, or use a standard Python installation from python.org.

## Full reference

The complete CLI command reference (every subcommand's flags and examples, environment
variables, checking the installed version) and the complete YAML model schema (Model
Structure, the `source`/`destination`/`processing`/`deployment` settings sections,
Databricks Asset Bundle output, Table YAML, Column Reference, the `environments`
runtime-detection block, Consumer Customization mechanisms, the Metadata API, and
`UpdateSchema` behaviour) live in the **`biontoolkit` Agent Skill** shipped with this
repo at `.claude/skills/biontoolkit/`:

- `SKILL.md` — overview and pointer, loaded automatically when relevant
- `references/yaml-cli-reference.md` — the full reference, loaded on demand

GitHub Copilot in VS Code discovers skills under `.claude/skills/` and will pull in the
full reference automatically when you're working with BionToolkit YAML files or CLI
commands — you don't need to open the file yourself. If your Copilot version doesn't
support Agent Skills yet, read
`.claude/skills/biontoolkit/references/yaml-cli-reference.md` directly for the complete
schema and CLI reference.

Run `biontoolkit update-docs` after upgrading to refresh this file and the skill's
reference together.

## Key Constraints

- Unity Catalog is **always required** — legacy `/mnt/` mount paths are not supported
- `DATABRICKS_TOKEN` must come from the environment — never commit it to YAML
- `<ENV:VARNAME>` tokens in YAML paths are resolved at deploy time
- Model directories must contain a `settings.yaml` to be valid
- ISO-folder high-watermark tables: never reset a table's `__iso_folder__` watermark to an
  older folder to reprocess history on an SCD2 table; reload it instead (empty the table and
  delete BOTH its `__iso_folder__` and `__file_path__` rows; see the skill
  reference's Upgrade notes, which also list the one-time effects of upgrading)
- Hash settings (`hash_function`, `int_key_hash_function`, `hash_multi_value_delimiter`,
  `global_null_key_replacement_function`) determine every stored
  surrogate key, foreign key, integer key and `ChangeHash` value: changing them (or upgrading
  past a release that changes their defaults, such as 0.5.58) means reloading dimensions and
  facts together; see the skill reference's Upgrade notes
- When installing from the private index, always pass `--index-url`, never
  `--extra-index-url` — the latter treats every configured index as equally trusted, so pip
  may resolve `biontoolkit`/`bion-etl-kernel` from a same-named public PyPI package instead of
  this one
