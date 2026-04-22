---
name: extract-and-load-restapi
description: End-to-end workflow for extracting data from a REST API and loading it into a Fabric Bronze Lakehouse as Delta tables. Use when the user asks to pull data from a REST API into Fabric (GitHub, Jira, Salesforce, etc.), build a new source-to-Bronze pipeline, or scaffold a Fabric PySpark notebook that calls an HTTP API. Drives the full process: requirements interrogation → API research → extraction-flow design → notebook build → test & verification → documentation.
---

# Skill: Extract & Load from a REST API into Fabric Bronze

This skill encapsulates the full workflow the repo uses for building a
REST-API → Bronze-Lakehouse extraction in Microsoft Fabric. Do **not**
jump to code. Walk through the phases below and the user will get a
predictable, documented, multi-environment, idempotent notebook.

## Authoritative references (read these first)

The two framework documents in `.claude/resources/` are the source of
truth. Read both before acting — they contain conventions this skill
assumes.

1. `.claude/resources/restapi-extraction-framework.md`
   — the **8-step framework** (requirements → API research → Postman
   → flow design → build → test → document → monitor).
2. `.claude/resources/fabric-notebook-coding-guide.md`
   — Fabric git-integration source format, kernel/metadata rules,
   Variable Library usage, AKV pattern, ABFS path construction,
   Delta MERGE conventions, helper-function and docstring style.
3. `CLAUDE.md` at the repo root
   — repo-specific warnings (do not hand-edit `.platform` GUIDs,
   preserve cell delimiters, `dependencies.lakehouse` GUIDs are
   environment-specific, etc.).

When you produce artifacts, cite which rule from these documents you
are applying. That both teaches the user and keeps you honest.

---

## Workflow

### Phase 1 — Play the consultant (framework Step 1)

Enter **plan mode** before writing anything. Use `AskUserQuestion` to
interrogate requirements rather than guessing. Never assume — ask. At
minimum cover:

- **Lesson / notebook code** — naming slot (e.g. `DE00X`).
- **Target entities / endpoints** — which resources? which fields?
- **Scope** — single record, single repo/project, an org, a tenant?
- **Auth** — PAT in AKV (default), OAuth client credentials, service
  principal, API key?
- **Target lakehouse** — which Bronze LH? schemas enabled? which
  schema? watermark/control tables go in a separate `meta` schema.
- **Load mode** — full refresh, incremental via `since`/watermark,
  CDC, append-only?
- **Variable Library** — confirm that every environment-specific
  value goes into the Variable Library so the notebook is
  DEV/TEST/PROD-portable.
- **Pre-reqs to produce instructions for** — PAT creation, AKV
  secret creation, Variable Library key list.

Iterate. Expect the user to refine the plan 2–4 times. Each
iteration, rewrite the plan file with the new decisions; don't patch
by hand. The plan file is the single source of truth you'll exit
plan mode with.

**Professional-advice framing:** when you present trade-offs
(e.g. full vs incremental, metadata-driven vs hard-coded), state the
recommendation first with a one-line "why", then the alternatives.
This mirrors the "play the consultant" tone of the framework Step 1.

### Phase 2 — API research (framework Step 2)

Before designing the flow:

- Link the official API docs in the plan.
- Confirm auth mechanism and required headers
  (e.g. `Authorization`, `Accept`, `X-*-Api-Version`, `User-Agent`).
- Identify rate limits and the headers that expose them
  (e.g. `X-RateLimit-Remaining`, `Retry-After`).
- For each target entity: endpoint, pagination mechanism
  (`Link rel="next"`, cursor, offset), incremental mechanism
  (`since`, `updated_at`, ETag).
- Capture one sample payload shape per entity to drive the explicit
  `StructType` projection.

Postman testing (framework Step 3) is the user's job; remind them
it's the fastest way to verify auth and to keep a reusable
request-by-request debug collection.

### Phase 3 — Design the extraction flow (framework Step 4)

Put an ASCII flow diagram in the plan file, phase by phase. Example
shape:

```
[Variable Library] → [PAT from AKV] → [ABFS base path]
── PHASE 1: full-refresh entity ─────────────────────────
  [GET listing] → paginate → flatten → [MERGE on key]
── PHASE 2: incremental entity ──────────────────────────
  [Read watermark] → per-parent GET with since → MERGE on key
  → [Update watermark]
[Verification & summary]
```

Decide watermark storage up front: a dedicated
`meta._load_watermarks` Delta table (not Delta metadata),
keyed by `entity_name`, upserted via MERGE only after a successful
load. Watermark advances on success only.

### Phase 4 — Build (framework Step 5)

Produce two files under `<Track>/<Code>_<Name>.Notebook/`:

1. `.platform` — `{"type":"Notebook","displayName":"…"}` with a
   **freshly generated GUID** in `config.logicalId`. Never reuse a
   GUID from another notebook (`CLAUDE.md`).
2. `notebook-content.py` — Fabric git-integration source format,
   kernel `synapse_pyspark`. Every `# CELL` is followed by a
   `# METADATA` block with `"language": "python"` and
   `"language_group": "synapse_pyspark"`. Markdown cells get **no**
   METADATA block. Preserve the exact delimiter lines (coding
   guide §1).

Cell layout (this shape has worked well and should be the default
— adjust per user's structure if they specify one):

1. Title markdown with TOC + pre-req checklist.
2. Step 0 — Setup: imports (`notebookutils` first), Variable
   Library, AKV token fetch, `ENTITIES` metadata dict,
   explicit `StructType` schemas, helpers.
3. Step 1..N — one markdown heading + one code cell per phase.
4. Final verification step: row counts, sample rows, watermark dump.

Required helpers (name them consistently across projects):

- `get_<system>_token()` — AKV lookup, only place secrets live.
- `build_headers(token)` — all static headers.
- `fetch_paginated(url, params)` — pagination + rate-limit handling.
- `flatten_<entity>(payload, extracted_at)` — Python-side JSON →
  flat dict projection (cleaner than Spark for deeply nested JSON
  at Bronze scale).
- `merge_to_delta(df, path, merge_key, table_fqn)` — first-write
  `save` + `CREATE TABLE IF NOT EXISTS … USING DELTA LOCATION '…'`,
  then `spark.sql(""" MERGE INTO … USING <tempview> ON … WHEN
  MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * """)`.
  Use the `spark.sql(""" … """)` style throughout, not the
  `DeltaTable.forPath(...).merge(...)` Python builder.
- `get_watermark(entity)` / `set_watermark(entity, ts)` — against
  `meta._load_watermarks`.

Conventions that are not negotiable (from the coding guide):

- All env values come from the Variable Library — no hardcoded
  workspace/lakehouse/AKV/API URLs.
- Secrets only via `notebookutils.credentials.getSecret` — never
  print, log, or persist the token.
- ABFS paths constructed from variables; schema-enabled lakehouses
  use `Tables/{schema}/{table}`.
- Explicit `StructType` on all writes (no schema inference at
  Bronze).
- Timestamps from the API stored as **STRING** in Bronze
  ("store as received"); Silver parses to `TIMESTAMP`.
- Idempotent by design: MERGE on stable natural keys; re-running
  is a no-op when upstream hasn't changed.

### Phase 5 — Test & verify (framework Step 6)

These notebooks cannot run on the dev machine. Verification happens
in Fabric, and the skill should *always* produce a verification
checklist in the plan and repeat it when handing off. Minimum set:

1. **Bootstrap check** — pre-reqs done, notebook attached to the
   target lakehouse, Variable Library bound to DEV.
2. **First run (cold)** — tables created; row counts match the
   source UI; watermark row exists.
3. **Second run (warm, no upstream change)** — "Incremental mode"
   message; zero or near-zero new rows; idempotent MERGE; watermark
   advances.
4. **Change detection** — make an upstream change, re-run, confirm
   the change lands and `_extracted_at` is newer.
5. **Full-refresh path** (if supported by parameters) — force full
   refresh; values unchanged on existing rows (idempotency).
6. **Back-fill / override watermark** — if the parameters cell
   exposes `override_watermark_*`, exercise it.
7. **Negative tests**:
   - bad secret name → clean Key Vault error at Step 0, no writes;
   - bad org / bad target → clean HTTP 404, existing data
     untouched;
   - rate-limit handling triggers sleep, not crash.
8. **Multi-env sanity** — switch Variable Library to TEST, rerun,
   writes land in TEST without code changes.

### Phase 6 — Document (framework Step 7)

The deliverable is itself documentation when it follows the coding
guide (title markdown with TOC, pre-reqs, embedded rationale in
Step 0 markdown, inline docstrings). On top of that:

- Keep the plan file (`~/.claude/plans/<slug>.md`) as the design
  artifact — it holds the flow diagram, decisions log, helper
  inventory, schema, verification checklist.
- Commit the plan alongside the notebook so the "why" isn't lost.
- Remind the user: if they built a Postman collection during
  Step 3, share it with the team — it's the fastest debug tool when
  the pipeline breaks later.

### Phase 7 — Monitor (framework Step 8, out of scope)

Call this out explicitly in the plan's "Out of scope" section.
Production monitoring, Admin-Lakehouse execution logging, retry
orchestration, unit tests around the flatten/merge helpers — note
these as future work so the user knows what's not covered.

---

## Communication style (how to collaborate during the build)

The prior run established these behaviours — keep them:

- **Ask, don't assume.** Use `AskUserQuestion` for every
  consequential choice (lesson code, scope, auth method, target
  lakehouse, load mode). Bundle questions (≤ 4 per call) and lead
  each with a recommended option + one-line rationale.
- **State the recommendation first.** Users want a default they can
  accept or redirect; they don't want a neutral list of options.
- **Surface trade-offs plainly** — e.g. "full vs incremental",
  "metadata dict vs hard-coded entity", "DeltaTable API vs
  `spark.sql`". Don't hide behind "it depends".
- **Rewrite the plan file each iteration.** Don't hand-patch it as
  the conversation evolves — it's the single source of truth and
  stays readable only if it's regenerated cleanly.
- **Exit plan mode before implementing.** Use `ExitPlanMode` only
  when the plan file reflects the latest decisions; don't ask for
  approval in prose.
- **When the user's message is garbled/truncated, ask.** Don't
  guess what they meant.

---

## Deliverables checklist

When the workflow is complete you should have produced:

- [ ] Plan file at `~/.claude/plans/<slug>.md` with context,
      decisions, pre-reqs, Variable Library table, flow diagram,
      cell structure, helper inventory, schema, verification
      checklist, out-of-scope list.
- [ ] New notebook folder `<Track>/<Code>_<Name>.Notebook/`
      containing:
  - [ ] `.platform` with a fresh `logicalId` GUID and correct
        `displayName`.
  - [ ] `notebook-content.py` in Fabric git-integration source
        format: kernel header, title markdown, Step 0 setup, one
        step per phase, final verification step. Every code cell
        has its METADATA block. No secrets, no hardcoded env
        values.
- [ ] A verification plan the user can run in Fabric after Git
      Integration sync.
- [ ] Explicit "Out of scope" note covering framework Step 8
      (monitoring) and anything else deferred.

If any item is missing, the workflow is not done.
