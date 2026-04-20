# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Educational Microsoft Fabric content for the [Fabric AI Workflows](https://skool.com/fabricai) community. Not an application — it is a collection of Fabric notebooks synced to git via **Fabric Git Integration**. There is no local build, no test suite, no package manager. Notebooks execute inside a Fabric workspace, not on a dev machine.

Top-level layout groups notebooks by track:
- `DA - Data Agents/` — lessons numbered `DA###`
- `DE - Data Engineering/` — lessons numbered `DE###`

## Fabric notebook source format

Each notebook is a directory named `<Lesson>.Notebook/` containing:
- `.platform` — JSON with the Fabric `displayName`, notebook `logicalId` (GUID), and schema reference. Do not invent or change `logicalId`; Fabric uses it to match git content to the workspace item.
- `notebook-content.py` — the notebook body in Fabric's git-integration source format.

`notebook-content.py` is **not a plain Python file** — Fabric parses it back into cells using these delimiters:

- `# CELL ********************` — start of a code cell
- `# MARKDOWN ********************` — start of a markdown cell (body is `#`-prefixed lines)
- `# METADATA ********************` followed by `# META { ... }` — per-cell metadata (language, language_group) or, at the top of the file, notebook-level metadata including the `dependencies.lakehouse` binding

When editing:
- Preserve the exact delimiter lines and the `# META` JSON blocks — breaking them corrupts the notebook on the next Fabric sync.
- The top-of-file `dependencies.lakehouse` block binds the notebook to a specific lakehouse by GUID (`default_lakehouse`, `default_lakehouse_workspace_id`). These GUIDs are environment-specific; don't "fix" them to match across notebooks unless you're intentionally rebinding.
- Per-cell language is set via `# META { "language": "python", "language_group": "synapse_pyspark" }`. Kernel is `synapse_pyspark`.

## Shared conventions across notebooks

- **Secrets**: SAS tokens are read at runtime from `Files/sas_credentials.json` in the attached lakehouse via `notebookutils.fs.head(...)`. Never inline a token in `notebook-content.py`. Notebooks mention Azure Key Vault as the recommended enterprise alternative.
- **External data source**: the shared demo dataset lives in storage account `adlswfsdm`, container `wfsdatamart`, accessed over `wasbs://` after setting `fs.azure.sas.<container>.<account>.blob.core.windows.net` on the Hadoop conf.
- **Fabric AI Functions**: `DE008` uses `synapse.ml.spark.aifunc`, which registers a `.ai` accessor on Spark DataFrames (e.g. `df.ai.analyze_sentiment(...)`, `df.ai.generate_response(...)`, `df.ai.translate(...)`). CU-cost printouts in these cells use the MS Fabric Text Analytics rate card — keep the formulas if you edit surrounding code.
- Output tables are written as Delta with `mode("overwrite")` and `option("overwriteSchema", "true")` via `saveAsTable(...)`.

## Working on notebooks

- Edit `notebook-content.py` directly; there is no Jupyter `.ipynb` in this repo.
- You cannot execute these notebooks locally — they depend on the Fabric runtime (`spark`, `notebookutils`, `synapse.ml.spark.aifunc`, and the bound lakehouse). Validate changes by reading the source; runtime verification happens after the user syncs the branch into Fabric.
- The typical workflow is: edit on a branch → PR into `main` → Fabric workspace pulls from `main` via git integration.
