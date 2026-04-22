# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### DE002 - GitHub Extraction to Bronze
#
# > Extracts Repositories and Commits from a GitHub organization via the
# > REST API and lands them in `DE002_Lakehouse` as Delta tables.
#
# #### In this notebook:
# - Step 0: Setup - imports, Variable Library, AKV token, helpers, schemas
# - Step 1: Extract Repositories (full refresh)
# - Step 2: Load Repositories (MERGE on id)
# - Step 3: Extract Commits (incremental via watermark)
# - Step 4: Load Commits (MERGE on sha)
# - Step 5: Update Watermarks
# - Step 6: Verification
#
# This notebook is dynamic: DEV/TEST/PROD are driven by the Variable
# Library `DE002_Variables`. Secrets come from Azure Key Vault.
#
# **Pre-requisites:**
# 1. Fine-grained GitHub PAT with `repo` (read) and `read:org` scopes
# 2. PAT stored in Azure Key Vault; Fabric workspace identity has
#    `Key Vault Secrets User`
# 3. Variable Library `DE002_Variables` with: `BRONZE_LH_NAME`,
#    `LH_WORKSPACE_NAME`, `AKV_URL`, `AKV_SECRET_NAME`, `GITHUB_ORG`,
#    `GITHUB_API_VERSION`

# MARKDOWN ********************

# #### Step 0: Setup

# CELL ********************

import json
import time
from datetime import datetime, timezone

import requests
import notebookutils

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("DE002_Variables")

LH_WORKSPACE_NAME = variables.LH_WORKSPACE_NAME
BRONZE_LH_NAME = variables.BRONZE_LH_NAME
AKV_URL = variables.AKV_URL
AKV_SECRET_NAME = variables.AKV_SECRET_NAME
GITHUB_ORG = variables.GITHUB_ORG
GITHUB_API_VERSION = variables.GITHUB_API_VERSION

BRONZE_BASE_PATH = (
    f"abfss://{LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/"
    f"{BRONZE_LH_NAME}.Lakehouse/Tables/"
)

GITHUB_SCHEMA = "github"
META_SCHEMA = "meta"

REPOS_TABLE = "gh_repositories"
COMMITS_TABLE = "gh_commits"
WATERMARKS_TABLE = "_load_watermarks"

REPOS_PATH = f"{BRONZE_BASE_PATH}{GITHUB_SCHEMA}/{REPOS_TABLE}"
COMMITS_PATH = f"{BRONZE_BASE_PATH}{GITHUB_SCHEMA}/{COMMITS_TABLE}"
WATERMARKS_PATH = f"{BRONZE_BASE_PATH}{META_SCHEMA}/{WATERMARKS_TABLE}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GITHUB_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

print(f"Org:           {GITHUB_ORG}")
print(f"Bronze base:   {BRONZE_BASE_PATH}")
print(f"Repos path:    {REPOS_PATH}")
print(f"Commits path:  {COMMITS_PATH}")
print(f"Watermarks:    {WATERMARKS_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_github_token() -> str:
    """Retrieve the GitHub PAT from Azure Key Vault.

    Returns:
        The PAT secret value. The value is never logged or written to
        any Delta column.
    """
    return notebookutils.credentials.getSecret(AKV_URL, AKV_SECRET_NAME)


GITHUB_TOKEN = get_github_token()
print("GitHub token retrieved from Key Vault.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ENTITIES = {
    "repositories": {
        "table": REPOS_TABLE,
        "path": REPOS_PATH,
        "merge_key": "id",
        "load_strategy": "full_refresh",
    },
    "commits": {
        "table": COMMITS_TABLE,
        "path": COMMITS_PATH,
        "merge_key": "sha",
        "load_strategy": "incremental",
    },
}

REPO_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("node_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("language", StringType(), True),
    StructField("stargazers_count", LongType(), True),
    StructField("forks_count", LongType(), True),
    StructField("open_issues_count", LongType(), True),
    StructField("watchers_count", LongType(), True),
    StructField("size", LongType(), True),
    StructField("default_branch", StringType(), True),
    StructField("visibility", StringType(), True),
    StructField("private", StringType(), True),
    StructField("fork", StringType(), True),
    StructField("archived", StringType(), True),
    StructField("disabled", StringType(), True),
    StructField("owner_login", StringType(), True),
    StructField("owner_id", LongType(), True),
    StructField("owner_type", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("pushed_at", StringType(), True),
    StructField("_extracted_at", TimestampType(), False),
])

COMMIT_SCHEMA = StructType([
    StructField("sha", StringType(), False),
    StructField("repo_full_name", StringType(), False),
    StructField("author_login", StringType(), True),
    StructField("author_name", StringType(), True),
    StructField("author_email", StringType(), True),
    StructField("author_date", StringType(), True),
    StructField("committer_login", StringType(), True),
    StructField("committer_name", StringType(), True),
    StructField("committer_email", StringType(), True),
    StructField("committer_date", StringType(), True),
    StructField("message", StringType(), True),
    StructField("parent_count", LongType(), True),
    StructField("html_url", StringType(), True),
    StructField("_extracted_at", TimestampType(), False),
])

WATERMARK_SCHEMA = StructType([
    StructField("entity_name", StringType(), False),
    StructField("last_loaded_at", StringType(), True),
    StructField("_updated_at", TimestampType(), False),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GITHUB_API = "https://api.github.com"


def build_headers(token: str) -> dict:
    """Build the standard GitHub REST API request headers.

    Args:
        token: GitHub PAT value.

    Returns:
        Dict of HTTP headers to send with every request.
    """
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": "fabric-de002-github-extraction",
    }


def _sleep_until_reset(headers: dict) -> None:
    """Sleep until the GitHub rate-limit reset time advertised in headers."""
    reset = headers.get("X-RateLimit-Reset")
    if reset is None:
        time.sleep(30)
        return
    wait = max(int(reset) - int(time.time()), 1) + 2
    print(f"Rate limit low. Sleeping {wait}s until reset.")
    time.sleep(wait)


def fetch_paginated(url: str, params: dict = None) -> list:
    """Fetch all pages from a GitHub REST endpoint.

    Follows the `Link: rel="next"` header. Pages of 100 items. Backs off
    when `X-RateLimit-Remaining` drops below 50. Raises on non-2xx
    status (except 409 on empty repos, which is caught by callers).

    Args:
        url: Absolute GitHub API URL.
        params: Query parameters for the first request. `per_page` is
            forced to 100.

    Returns:
        List of JSON items concatenated across all pages.
    """
    headers = build_headers(GITHUB_TOKEN)
    params = dict(params or {})
    params.setdefault("per_page", 100)

    results = []
    next_url = url
    next_params = params
    while next_url:
        resp = requests.get(next_url, headers=headers, params=next_params, timeout=60)
        if resp.status_code == 409:
            resp.raise_for_status()
        if resp.status_code >= 400:
            raise RuntimeError(
                f"GitHub API error {resp.status_code} for {next_url}: "
                f"{resp.text[:500]}"
            )
        page = resp.json()
        if isinstance(page, list):
            results.extend(page)
        else:
            results.append(page)

        remaining = int(resp.headers.get("X-RateLimit-Remaining", "5000"))
        if remaining < 50:
            _sleep_until_reset(resp.headers)

        link = resp.headers.get("Link", "")
        next_url = None
        next_params = None
        for part in link.split(","):
            segment = part.strip()
            if segment.endswith('rel="next"'):
                next_url = segment[segment.find("<") + 1 : segment.find(">")]
                break
    return results


def flatten_repo(repo: dict, extracted_at: datetime) -> dict:
    """Project a GitHub repo JSON payload onto the Bronze repo schema."""
    owner = repo.get("owner") or {}
    return {
        "id": repo.get("id"),
        "node_id": repo.get("node_id"),
        "name": repo.get("name"),
        "full_name": repo.get("full_name"),
        "description": repo.get("description"),
        "language": repo.get("language"),
        "stargazers_count": repo.get("stargazers_count"),
        "forks_count": repo.get("forks_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "watchers_count": repo.get("watchers_count"),
        "size": repo.get("size"),
        "default_branch": repo.get("default_branch"),
        "visibility": repo.get("visibility"),
        "private": str(repo.get("private")) if repo.get("private") is not None else None,
        "fork": str(repo.get("fork")) if repo.get("fork") is not None else None,
        "archived": str(repo.get("archived")) if repo.get("archived") is not None else None,
        "disabled": str(repo.get("disabled")) if repo.get("disabled") is not None else None,
        "owner_login": owner.get("login"),
        "owner_id": owner.get("id"),
        "owner_type": owner.get("type"),
        "html_url": repo.get("html_url"),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
        "_extracted_at": extracted_at,
    }


def flatten_commit(commit: dict, repo_full_name: str, extracted_at: datetime) -> dict:
    """Project a GitHub commit JSON payload onto the Bronze commit schema."""
    c = commit.get("commit") or {}
    author = c.get("author") or {}
    committer = c.get("committer") or {}
    author_user = commit.get("author") or {}
    committer_user = commit.get("committer") or {}
    parents = commit.get("parents") or []
    return {
        "sha": commit.get("sha"),
        "repo_full_name": repo_full_name,
        "author_login": author_user.get("login"),
        "author_name": author.get("name"),
        "author_email": author.get("email"),
        "author_date": author.get("date"),
        "committer_login": committer_user.get("login"),
        "committer_name": committer.get("name"),
        "committer_email": committer.get("email"),
        "committer_date": committer.get("date"),
        "message": c.get("message"),
        "parent_count": len(parents),
        "html_url": commit.get("html_url"),
        "_extracted_at": extracted_at,
    }


def merge_to_delta(df, path: str, merge_key: str, table_fqn: str) -> None:
    """Create the Delta table on first write or MERGE on re-runs.

    Uses `spark.sql(""" ... """)` MERGE syntax against a temp view of
    the source DataFrame. Target table is registered at `table_fqn` so
    SQL can reference it by name.

    Args:
        df: Source Spark DataFrame.
        path: ABFS location of the Delta table.
        merge_key: Column name used in the MERGE predicate.
        table_fqn: Fully-qualified target name, e.g. `github.gh_repositories`.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_fqn} USING DELTA LOCATION '{path}'"
        )
        print(f"Initial write: {table_fqn} ({df.count()} rows)")
        return

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_fqn} USING DELTA LOCATION '{path}'"
    )
    view = f"src_{merge_key}_{abs(hash(table_fqn))}"
    df.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {table_fqn} AS t
        USING {view} AS s
        ON t.{merge_key} = s.{merge_key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"MERGE into {table_fqn} complete ({df.count()} source rows).")


def get_watermark(entity: str):
    """Return `last_loaded_at` ISO string for entity, or None if unset."""
    if not DeltaTable.isDeltaTable(spark, WATERMARKS_PATH):
        return None
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {META_SCHEMA}.{WATERMARKS_TABLE} "
        f"USING DELTA LOCATION '{WATERMARKS_PATH}'"
    )
    row = spark.sql(f"""
        SELECT last_loaded_at
        FROM {META_SCHEMA}.{WATERMARKS_TABLE}
        WHERE entity_name = '{entity}'
    """).collect()
    return row[0]["last_loaded_at"] if row else None


def set_watermark(entity: str, last_loaded_at: str) -> None:
    """Upsert a watermark row for `entity`."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = spark.createDataFrame(
        [Row(entity_name=entity, last_loaded_at=last_loaded_at, _updated_at=now)],
        schema=WATERMARK_SCHEMA,
    )
    merge_to_delta(
        df,
        WATERMARKS_PATH,
        "entity_name",
        f"{META_SCHEMA}.{WATERMARKS_TABLE}",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1: Extract Repositories
#
# Full refresh. Org repo list is small and fields like stars and forks
# drift over time, so every run pulls the full list.

# CELL ********************

repos_extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)

raw_repos = fetch_paginated(
    f"{GITHUB_API}/orgs/{GITHUB_ORG}/repos",
    params={"type": "all", "sort": "updated", "direction": "desc"},
)
print(f"Fetched {len(raw_repos)} repositories from org '{GITHUB_ORG}'.")

repo_rows = [flatten_repo(r, repos_extracted_at) for r in raw_repos]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Load Repositories

# CELL ********************

repos_df = spark.createDataFrame(repo_rows, schema=REPO_SCHEMA)
merge_to_delta(
    repos_df,
    REPOS_PATH,
    merge_key="id",
    table_fqn=f"{GITHUB_SCHEMA}.{REPOS_TABLE}",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Extract Commits (incremental)
#
# Reads the `commits` watermark. Fetches each repo's commits with
# `since={watermark}`; empty repos return 409 and are skipped. First
# run (no watermark) pulls full history bounded by the API.

# CELL ********************

commits_extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
run_start_iso = commits_extracted_at.strftime("%Y-%m-%dT%H:%M:%SZ")

watermark = get_watermark("commits")
if watermark:
    print(f"Incremental mode. Fetching commits since {watermark}.")
else:
    print("No watermark found. Fetching full commit history for each repo.")

commit_rows = []
skipped_empty = 0
for r in raw_repos:
    repo_full_name = r["full_name"]
    url = f"{GITHUB_API}/repos/{repo_full_name}/commits"
    params = {}
    if watermark:
        params["since"] = watermark
    try:
        raw_commits = fetch_paginated(url, params=params)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 409:
            skipped_empty += 1
            continue
        raise
    except RuntimeError as e:
        if "409" in str(e):
            skipped_empty += 1
            continue
        raise
    for c in raw_commits:
        commit_rows.append(
            flatten_commit(c, repo_full_name, commits_extracted_at)
        )

print(
    f"Fetched {len(commit_rows)} commits across {len(raw_repos) - skipped_empty} "
    f"repos (skipped {skipped_empty} empty)."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Load Commits

# CELL ********************

if commit_rows:
    commits_df = spark.createDataFrame(commit_rows, schema=COMMIT_SCHEMA)
    merge_to_delta(
        commits_df,
        COMMITS_PATH,
        merge_key="sha",
        table_fqn=f"{GITHUB_SCHEMA}.{COMMITS_TABLE}",
    )
else:
    print("No new commits to load.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 5: Update Watermarks
#
# Advance the `commits` watermark only if Step 4 succeeded.

# CELL ********************

set_watermark("commits", run_start_iso)
print(f"Watermark 'commits' set to {run_start_iso}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 6: Verification

# CELL ********************

print("Row counts:")
spark.sql(f"SELECT COUNT(*) AS n FROM {GITHUB_SCHEMA}.{REPOS_TABLE}").show()
spark.sql(f"SELECT COUNT(*) AS n FROM {GITHUB_SCHEMA}.{COMMITS_TABLE}").show()

print("Sample repositories:")
spark.sql(f"""
    SELECT full_name, language, stargazers_count, forks_count, updated_at
    FROM {GITHUB_SCHEMA}.{REPOS_TABLE}
    ORDER BY stargazers_count DESC
    LIMIT 10
""").show(truncate=False)

print("Sample commits:")
spark.sql(f"""
    SELECT repo_full_name, author_login, author_date, substr(message, 1, 80) AS msg
    FROM {GITHUB_SCHEMA}.{COMMITS_TABLE}
    ORDER BY author_date DESC
    LIMIT 10
""").show(truncate=False)

print("Watermarks:")
spark.sql(f"SELECT * FROM {META_SCHEMA}.{WATERMARKS_TABLE}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
