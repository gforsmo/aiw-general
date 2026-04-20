# Fabric Notebook Coding Guide

> This document is for an LLM coding agent. Follow these conventions exactly when writing Microsoft Fabric notebook code.

---

## 1. Fabric Notebook File Format

A Fabric notebook is a **directory** named `<notebook-name>.Notebook/` containing:

- `.platform` — JSON metadata (display name, logical ID, schema version)
- `notebook-content.py` — all code, markdown, and metadata in a single flat Python file

### Cell Delimiters

The `notebook-content.py` file uses comment-based delimiters to separate cells. There are three cell types:

**Code cell:**

```python
# CELL ********************

# your python code here
x = 1 + 1
```

**Markdown cell:**

```python
# MARKDOWN ********************

# #### This is a heading
#
# This is a paragraph of markdown. Each line is prefixed with `# `.
# - Bullet points work like this
# - Another bullet
```

**Parameters cell** (for pipeline-invoked notebooks):

```python
# PARAMETERS CELL ********************

status = ""
pipeline_id = ""
```

### Metadata Blocks

Every code cell and parameters cell MUST be followed by a metadata block. Markdown cells do NOT get metadata blocks.

```python
# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

**Kernel types** — use the correct `language_group` based on the notebook kernel:

- PySpark notebooks: `"language_group": "synapse_pyspark"`, kernel `"name": "synapse_pyspark"`
- Python (non-Spark) notebooks: `"language_group": "jupyter_python"`, kernel `"name": "jupyter"` with `"jupyter_kernel_name": "python3.11"`

### File-level Metadata

The file MUST start with:

```python
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }
```

### Complete Cell Sequence Example

```python
# MARKDOWN ********************

# #### Step 1: Get the data

# CELL ********************

data = get_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Process the data

# CELL ********************

result = process(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

Key rules:

- `# MARKDOWN` is NEVER followed by a `# METADATA` block
- `# CELL` is ALWAYS followed by a `# METADATA` block (after the code)
- Blank lines between delimiters and code are fine but keep it consistent

---

## 2. Notebook Structure Pattern

Every notebook follows this layout:

1. **Title markdown cell** — project ID, notebook purpose, table of contents
2. **Step 0: Setup** — imports, variable library, helper functions
3. **Step N cells** — each step has a markdown heading then one or more code cells
4. Steps are numbered sequentially and named descriptively

### Title Cell Convention

```python
# MARKDOWN ********************

# #### Notebook Name & purpose
#
# > Brief description of context and intent.
#
# #### In this notebook:
# - Step 0: Solution Set Up - imports, helper functions
# - Step 1: Description of first operation
# - Step 2: Description of second operation
#
# This notebook is dynamic: it can be run in DEV, TEST and PROD, thanks to the use of Variable libraries.
```

### Markdown Documentation Style

- Use `####` for step headings (not `#` or `##` — those are for top-level sections)
- Add a brief paragraph before code cells explaining the "why", not the "what"
- Use markdown cells to explain cleaning strategies, loading logic, and design decisions
- Use blockquotes (`>`) for context/caveats
- Keep markdown cells short — 2-5 lines max

---

## 3. Security & Key Management

**Never hardcode secrets.** Use Azure Key Vault via `notebookutils`:

```python
def get_secret_from_akv() -> str:
    akv_name = 'https://<vault-name>.vault.azure.net/'
    secret_name = '<secret-name>'
    api_key = notebookutils.credentials.getSecret(akv_name, secret_name)
    return api_key
```

- Wrap secret retrieval in a named function with a return type hint
- AKV URL and secret name can be inline (they are not secrets themselves)
- The actual secret value is only held in a variable, never printed or logged
- If different vaults exist for different environments- use the Variable Library to store the AKV name and Secret Name, for all deployment environments.

---

## 4. Environment Configuration & Scalability

### Variable Libraries

Use Fabric Variable Libraries to make notebooks environment-agnostic (DEV/TEST/PROD):

```python
variables = notebookutils.variableLibrary.getLibrary("variable-library-name")
ws_name = variables.DATASTORE_WORKSPACE_NAME
lh_name = variables.BRONZE_LH_NAME
```

- Load the variable library once at the top of the notebook
- Access variables as properties (e.g. `variables.BRONZE_LH_NAME`)
- Never hardcode workspace names, lakehouse names, or workspace IDs

### ABFS Path Construction

For reading from and writing to Lakehouses, always use the ABFS path. Always construct ABFS paths dynamically from variables, so that the notebook code works across different deployment environments:

```python
def construct_abfs_path(layer="bronze"):
    layer_mapping = {
        "bronze": variables.BRONZE_LH_NAME,
        "silver": variables.SILVER_LH_NAME,
        "gold": variables.GOLD_LH_NAME
    }
    ws_name = variables.LH_WORKSPACE_NAME
    lh_name = layer_mapping.get(layer)
    return f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/"
```

#### Lakehouses with and without Schemas

> Important: The ABFS path suffix will be different for Lakehouses with Schemas Enabled, and those without.

```python
full_path_to_lh_with_schemas = f"{base_abfs_path}Tables/{schema_name}/{table_name}"
full_path_to_lh_without_schemas = f"{base_abfs_path}Tables/{table_name}"
```

If using Lakehouses with Schemas, you can use the four-part naming syntax (not the use of backticks which are required if workspace name contains special characters):

```python
result = spark.sql("""SELECT * FROM `{WORKSPACE_NAME}`.`{LAKEHOUSE_NAME}.`{SCHEMA_NAME}`.`{TABLE-NAME}`
```

### Metadata-Driven Design

Use Python dicts (or SQL database) to store entity metadata, making it easy to add new entities without changing logic:

```python
METADATA = {
    "datapoint1": {
        "base_url": "https://api.example.com/channels",
        "base_params": "part=snippet,statistics",
        "write_location": "path/to_folder/"
    },
    "datapoint2": { ... }
}
```

## 6. Delta Table Operations

### Always use MERGE for idempotency

```python
delta_table = DeltaTable.forPath(spark, full_write_path)

(
    delta_table.alias("target")
    .merge(df.alias("source"), "target.id = source.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

- For simple Bronze/Silver loads, use `whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()`
- For Gold dimension tables, use explicit column mappings in `whenMatchedUpdate(set={})` and `whenNotMatchedInsert(values={})`

### Check table existence before first write

```python
if DeltaTable.isDeltaTable(spark, path):
    # MERGE
else:
    df.write.format("delta").save(path)
```

---

## 7. Logging & Observability

- Pipeline execution logging: accept parameters from calling pipeline (`# PARAMETERS CELL`), parse output JSON, write structured log record to Admin Lakehouse
- Use explicit Spark schemas (`StructType`/`StructField`) for log DataFrames
- Separate success/failure parsing logic with clear field mappings

---

## 8. Helper Function Conventions

- Define all helper functions in Step 0
- Every function has a Google-style docstring with Args/Returns
- Functions are small and single-purpose
- Reusable read/write functions accept `base_abfs_path`, `schema`, `table_name`
- API pagination is handled in a dedicated function with a `while True` / `break` loop
- Batch large API calls (e.g. 40 items per batch) to respect API limits

---

## 9. Coding Style Summary

| Convention       | Rule                                                                                           |
| ---------------- | ---------------------------------------------------------------------------------------------- |
| Imports          | Top of notebook, one `import` per line, `notebookutils` always first                           |
| Naming           | `snake_case` for variables/functions, `UPPER_CASE` for constants/metadata dicts                |
| Notebook naming  | `nb-{project}-{step_number}-{verb}-{entity}.Notebook`                                          |
| Variable library | Loaded once in setup, accessed via `variables.PROPERTY_NAME`                                   |
| Paths            | Always ABFS, always constructed from variables, never hardcoded                                |
| Secrets          | Always from Azure Key Vault via `notebookutils.credentials.getSecret()`                        |
| Table writes     | Always Delta MERGE for idempotency                                                             |
| Spark reads      | `spark.read.format("delta").load(path)` or `spark.read.option("multiLine", "true").json(path)` |
| Docstrings       | Google-style, on all functions                                                                 |
| Markdown         | Before every logical step, brief explanation of "why"                                          |
| Error handling   | Idempotent patterns preferred over try/catch (e.g. `CREATE IF NOT EXISTS`, MERGE)              |
