---
name: rest-api-extraction-fabric
description: "Use this skill whenever the task involves extracting data from a REST API and loading it into a Microsoft Fabric Lakehouse (Bronze layer). Triggers include: building Fabric notebooks for API ingestion, paginated REST API calls, Delta MERGE writes to OneLake, watermark-based incremental loads, Key Vault credential retrieval, Variable Library configuration, and ABFS path construction. Also use when the user asks to set up an extraction pipeline from GitHub, Jira, SharePoint, or any other REST API into Fabric."
---

# REST API Extraction to Microsoft Fabric Lakehouse

## Overview

This skill encapsulates a complete, professional workflow for extracting data from any REST API and loading it into a Microsoft Fabric Lakehouse. It is grounded in two resource documents that **must** be read at the start of every invocation:

| Resource | Path | Purpose |
|---|---|---|
| Fabric Notebook Coding Guide | `.claude/resources/fabric-notebook-coding-guide.md` | Defines notebook file format, cell delimiters, metadata blocks, security patterns, Variable Libraries, ABFS paths, Delta MERGE, helper function style, and coding conventions |
| REST API Extraction Framework | `.claude/resources/restapi-extraction-framework.md` | 8-step methodology: requirements, API research, Postman testing, flow planning, build, test/reinforce, document, monitor |

**Before doing anything else**, read both files in full. They are your source of truth for how code should be written and how the project should be run.

---

## Phase 1: Research & Requirements (Framework Steps 1-2)

> Reference: `restapi-extraction-framework.md` Steps 1 and 2

You are acting as a consultant. Your job is to ask the right questions before writing a single line of code. Use `AskUserQuestion` to gather requirements interactively.

### Questions to ask the client

**Data scope:**
- Which entities/tables does the business need from this API? (e.g. for GitHub: repos, commits, issues, PRs)
- Warn them: "the more tables, the longer the development time" — but also don't be too lean (rework is expensive)

**Naming & placement:**
- Which lesson/course code should this use? (e.g. DE002, DE005)
- Should we create a new notebook or use an existing one?
- Should we create a new Lakehouse or use an existing one?

**Environment & config:**
- Should we use Variable Libraries for DEV/TEST/PROD portability? (recommend yes)
- Is there an existing Variable Library, or do we need to define one?

**Authentication:**
- Is the API key / token already stored in Azure Key Vault?
- If not, provide setup instructions (see Phase 2 deliverables below)

**Load strategy:**
- Does the API support incremental extraction (e.g. `since`, `updated_after` params)?
- Which entities should be incremental vs full refresh?

**API constraints:**
- Are there rate limits? Pagination requirements?
- Are there nested endpoints (e.g. must fetch projects, then tasks per project)?

### Deliverables from Phase 1

- A clear list of entities to extract and key columns for each
- The notebook name, Lakehouse name, and lesson code
- A decision on Variable Libraries and Key Vault configuration
- A go/no-go decision: can the API satisfy the requirements?

---

## Phase 2: API Documentation & Auth Setup (Framework Step 3)

> Reference: `restapi-extraction-framework.md` Steps 2 and 3

### Research the API

Use `WebFetch` or `WebSearch` to find and read the API documentation. Answer:

1. **Authentication method**: API key, OAuth, PAT, client credentials?
2. **Endpoints needed**: Which endpoints return the required data? What params/headers do they need?
3. **Response structure**: What does the JSON response look like? Which fields to extract?
4. **Pagination**: Link header? Cursor-based? Offset? Page number?
5. **Rate limits**: Requests per hour/minute? How are limits communicated (headers)?
6. **Incremental support**: Does the API accept `since`, `updated_after`, or similar filters?

### Provide auth setup instructions

If the client needs to create credentials, provide step-by-step instructions:

1. How to generate the API key/token in the source system
2. How to store it in Azure Key Vault:
   - Create (or navigate to) a Key Vault in Azure Portal
   - Create a secret with a descriptive name
   - Grant the Fabric workspace identity the **Key Vault Secrets User** role
   - Record the vault URI
3. What variables to add to the Variable Library

### Provide Variable Library specification

Define the exact variables needed. Always include at minimum:

| Variable | Purpose |
|---|---|
| `BRONZE_LH_NAME` | Target Lakehouse name |
| `LH_WORKSPACE_NAME` | Workspace name for ABFS path construction |
| `AKV_URL` | Azure Key Vault URL |
| `AKV_SECRET_NAME` | Secret name for the API credential |
| Any API-specific config | e.g. `GITHUB_ORG`, `JIRA_DOMAIN`, etc. |

---

## Phase 3: Plan the Extraction Flow (Framework Step 4)

> Reference: `restapi-extraction-framework.md` Step 4

Before writing code, design the extraction flow as visual pseudocode. Present it to the client for review. This serves as both a development guide and documentation.

### Flow diagram format

Use an ASCII flow diagram showing the sequential steps:

```
[Load config] → [Get API token from Key Vault] → [Build ABFS paths]
    ↓
== ENTITY 1 (full/incremental) ==
    ↓
[API call with pagination] → [Flatten JSON] → [Spark DF] → [MERGE into Delta]
    ↓
== ENTITY 2 (full/incremental) ==
    ↓
[Read watermark] → [API call with since param] → [Flatten] → [MERGE]
    ↓
[Update watermarks] → [Verification]
```

### Key decisions to document in the flow

- Which entities are full refresh vs incremental (and why)
- How pagination is handled
- How rate limits are respected
- How errors (4xx, 5xx) are handled per endpoint
- Where the watermark table lives and what schema it uses

---

## Phase 4: Build the Notebook (Framework Step 5)

> Reference: `fabric-notebook-coding-guide.md` for ALL coding conventions

### Notebook file structure

Every Fabric notebook is a directory:

```
{NotebookName}.Notebook/
├── .platform          (JSON: type, displayName, logicalId)
└── notebook-content.py (all code, markdown, metadata)
```

### Cell format rules (non-negotiable)

These rules come directly from `fabric-notebook-coding-guide.md`:

- File starts with `# Fabric notebook source` then a file-level `# METADATA` block
- `# MARKDOWN` cells are NEVER followed by a `# METADATA` block
- `# CELL` (code) cells are ALWAYS followed by a `# METADATA` block
- Use `synapse_pyspark` for PySpark notebooks
- Use `####` for step headings in markdown cells

### Notebook structure pattern

Follow this layout exactly:

| Cell | Type | Content |
|---|---|---|
| 0 | markdown | Title: notebook name, description, table of contents |
| 1 | markdown | Step 0 heading |
| 2 | code | Imports (notebookutils first, one per line) |
| 3 | code | Load Variable Library, construct ABFS base path |
| 4 | code | Retrieve API credential from Key Vault |
| 5 | code | ENTITIES metadata dict + Spark StructType schemas |
| 6 | code | All helper functions |
| N | markdown + code | Step 1..N: Extract entity, Load entity (one pair per entity) |
| N+1 | markdown + code | Update watermarks |
| N+2 | markdown + code | Verification and summary |

### Coding conventions checklist

From `fabric-notebook-coding-guide.md`:

- [ ] **Variable Library**: loaded once in Step 0, accessed via `variables.PROPERTY_NAME`
- [ ] **Secrets**: always from Key Vault via `notebookutils.credentials.getSecret()`
- [ ] **ABFS paths**: constructed dynamically from variables, never hardcoded
- [ ] **ABFS path must include schema folder**: e.g. `Tables/dbo/` not just `Tables/`
- [ ] **Delta writes**: always MERGE for idempotency (`DeltaTable.isDeltaTable` check first)
- [ ] **Metadata-driven**: entity config in a Python dict (ENTITIES), easy to extend
- [ ] **Helper functions**: defined in Step 0, Google-style docstrings, single-purpose
- [ ] **Markdown cells**: before every step, explain the "why" not the "what"
- [ ] **Naming**: `snake_case` for variables/functions, `UPPER_CASE` for constants

### Helper function patterns

Every extraction notebook needs these reusable functions:

1. `build_headers()` — Construct auth + content-type headers
2. `fetch_paginated(url, params)` — Handle pagination + rate limits automatically
3. `flatten_{entity}(raw, timestamp)` — Extract fields from nested JSON, add `_extracted_at`
4. `merge_to_delta(df, path, merge_key)` — Idempotent write (initial save or MERGE)
5. `get_watermark(entity_name)` — Read last-loaded timestamp from watermark table
6. `set_watermark(entity_name, timestamp)` — Upsert watermark after successful load

### ABFS path construction

```python
ABFS_BASE_PATH = f"abfss://{LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{BRONZE_LH_NAME}.Lakehouse/Tables/dbo/"
```

Always include the schema folder (`dbo/`, `meta/`, etc.) in the path. Without it, Fabric Lakehouses with schemas enabled will misinterpret the table folder as a schema name.

### Delta MERGE pattern

```python
if DeltaTable.isDeltaTable(spark, path):
    delta_table = DeltaTable.forPath(spark, path)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df.write.format("delta").save(path)
```

### Incremental load pattern

```python
watermark = get_watermark("entity_name")
params = {}
if watermark:
    params["since"] = watermark   # or updated_after, etc.

raw_data = fetch_paginated(url, params=params)
# ... flatten, merge ...
set_watermark("entity_name", extraction_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"))
```

---

## Phase 5: Test, Reinforce & Document (Framework Steps 6-8)

> Reference: `restapi-extraction-framework.md` Steps 6-8

### Testing checklist

- [ ] Run full load end-to-end, verify row counts
- [ ] Run incremental load — confirm only new records are fetched
- [ ] Simulate rate limit — confirm backoff/sleep logic activates
- [ ] Simulate 404/409 on empty repo/entity — confirm it is skipped gracefully
- [ ] Rerun idempotency test — confirm no duplicates in Delta tables
- [ ] Verify watermark table is updated correctly after each run

### Documentation to add to notebook

Add a markdown cell at the top with:
- What this notebook does and why
- Source API and authentication method
- Entities extracted and their merge keys
- Load strategy per entity (full vs incremental)
- Known limitations or caveats

### Monitoring reminders

- Schedule the notebook via Fabric Data Factory pipeline
- Set alerts on pipeline failure
- Periodically review watermark table to confirm incremental loads are progressing
