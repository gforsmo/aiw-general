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
# > Extracts Repositories and Commits from a GitHub organization via the
# > REST API and lands them in `DE002_Lakehouse` as Delta tables.
# #### In this notebook:
# - Step 0: Setup - imports, Variable Library, AKV token, helpers, schemas
# - Step 1: Extract Repositories (full refresh)
# - Step 2: Load Repositories (MERGE on id)
# - Step 3: Extract Commits (incremental via watermark)
# - Step 4: Load Commits (MERGE on sha)
# - Step 5: Update Watermarks
# - Step 6: Verification
# This notebook is dynamic: DEV/TEST/PROD are driven by the Variable
# Library `DE002_Variables`. Secrets come from Azure Key Vault.


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

# MARKDOWN ********************

# #### Load Variable Library and contruct paths
# The variable library makes this notebook environment-agnostic. All configuration values ...

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("DE002_Variables")

LH_WORKSPACE_NAME = variables.LH_WORKSPACE_NAME
BRONZE_LH_NAME = variables.BRONZE_LH_NAME
AKV_URL = variables.AKV_URL
AKV_SECRET_NAME = variables.AKV_SECRET_NAME
GITHUB_ORG = variables.GITHUB_ORG
GITHUB_API_VERSION = variables.GITHUB_API_VERSION

ABFS_BASE_PATH = (
    f"abfss://{LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/"
    f"{BRONZE_LH_NAME}.Lakehouse/Tables/dbo/"
)

print(f"Github org:           {GITHUB_ORG}")
print(f"Bronze base:   {ABFS_BASE_PATH}")




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

# MARKDOWN ********************

# #### Metadata-driven configuration and schemas
# Using a metadata dict makes it easy to add new entities int the future without changing 
# the core extration logic

# CELL ********************



ENTITIES = {
    "gh_repositories": {
        "endpoint": f"https://api.github.com/users/{GITHUB_ORG}/repos",
        "merge_key": "id",
        "write_path": f"{ABFS_BASE_PATH}gh_repositories",
    },
    "gh_commits": {
        "endpoint_template": "https://api.github.com/repos/{owner}/{repo}/commits",  # ← endret
        "merge_key": "sha",
        "write_path": f"{ABFS_BASE_PATH}gh_commits",
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

# MARKDOWN ********************

# #### Helper functions

# CELL ********************



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


def merge_to_delta(df, path: str, merge_key: str):
    """Write a DataFrame to a Delta table using MERGE for idempotency.

    If the table does not yet exist, performs an initial write.
    If the table exists, merges on the specified key.

    Args:
        df: Spark DataFrame to write.
        path: Full ABFS path to the Delta table.
        merge_key: Column name to match on for upsert.
    """
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    
        print (f" MERGE complete into {path} ({df.count()} source rows)")
    else:
        df.write.format("delta").save(path)
        print(f" Initial write to {path} ({df.count()} rows)")


def get_watermark(entity_name: str) -> str:
    """Read the last loaded timestamp for an entity from the meta._load_watermarks table.
    Args:
    entity_name: The entity identifier (e.g. "commits").
    Returns:
 
    str: ISO 8601 timestamp string, or None if no watermark exists.
    """ 
    try:
        df = spark.sql(f"""
            SELECT last_loaded_at
            FROM `{LH_WORKSPACE_NAME}`.{BRONZE_LH_NAME}.meta._load_watermarks
            WHERE entity_name = '{entity_name}'
        """)
        row = df.first()
        if row:
            return row["last_loaded_at"]
    except Exception:
        pass
    return None

def set_watermark(entity_name: str, last_loaded_at: str) -> None:
    """Write or update the watermark for an entity in the meta schema.
    
    Creates the meta schema and _load_watermarks table if they do not exist, 
    then performs a MERGE to upsert the watermark row.

    Args:
        entity_name: The entity identifier (e.g. "commits"). 
        last_loaded_at: ISO 8601 timestamp to record.
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{LH_WORKSPACE_NAME}`.{BRONZE_LH_NAME}.meta._load_watermarks (
            entity_name STRING NOT NULL,
            last_loaded_at STRING NOT NULL,
            _updated_at TIMESTAMP NOT NULL
        )
        USING DELTA
    """)

    spark.sql(f"""
        MERGE INTO `{LH_WORKSPACE_NAME}`.{BRONZE_LH_NAME}.meta._load_watermarks AS target
        USING (
            SELECT '{entity_name}' AS entity_name,
                   '{last_loaded_at}' AS last_loaded_at,
                   current_timestamp() AS _updated_at
        ) AS source
        ON target.entity_name = source.entity_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Watermark updated: {entity_name} -> {last_loaded_at}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1: Extract Repositories
# Full refresh. Org repo list is small and fields like stars and forks
# drift over time, so every run pulls the full list.

# CELL ********************

extraction_timestamp = datetime.now(timezone.utc)
raw_repos = fetch_paginated (ENTITIES ["gh_repositories"]["endpoint"]) 
print (f"Extracted {len (raw_repos)} repositories from {GITHUB_ORG}")
repo_rows = [flatten_repo(r, extraction_timestamp) for r in raw_repos]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Load Repositories into Bronze Lakehouse
# We create a Spark Data frame from the flattened repository data and MERGE it into the gh_repositories Delta table. The merge key is the
# GitHub Repo id which is immutable
# Existing rows are updated, new repos are inserted

# CELL ********************

df_repos = spark.createDataFrame (repo_rows, schema=REPO_SCHEMA)
display(df_repos)

merge_to_delta(
    df_repos,
    ENTITIES["gh_repositories"]["write_path"], 
    ENTITIES["gh_repositories"]["merge_key"],
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Extract Commits (incremental)
# Reads the `commits` watermark. Fetches each repo's commits with
# `since={watermark}`; empty repos return 409 and are skipped. First
# run (no watermark) pulls full history bounded by the API.

# CELL ********************

watermark = get_watermark("gh_commits")
if watermark:
    print(f"Incremental mode. Fetching commits since {watermark}.")
else:
    print("No watermark found. Fetching full commit history for each repo.")

repo_names = [r["full_name"] for r in repo_rows]

all_commit_rows = []
errors = []

for idx, repo_full_name in enumerate(repo_names):
    owner, repo = repo_full_name.split("/")
    url = ENTITIES["gh_commits"]["endpoint_template"].format(owner=owner, repo=repo)

    params = {}
    if watermark:
        params["since"] = watermark

    try:
        raw_commits = fetch_paginated(url, params=params)
        commit_rows = [flatten_commit(c, repo_full_name, extraction_timestamp) for c in raw_commits]
        all_commit_rows.extend(commit_rows)

        if (idx + 1) % 10 == 0 or (idx + 1) == len(repo_names):
            print(f"Progress: {idx + 1}/{len(repo_names)} repos processed, {len(all_commit_rows)} commits so far.")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            pass  # Tom/utilgjengelig repo, ignorer
        else:
            errors.append({"repo": repo_full_name, "error": str(e)})
            print(f"WARNING: Error fetching commits for {repo_full_name}: {e}")

print(f"\nExtraction complete: {len(all_commit_rows)} commits from {len(repo_names)} repos.")
if errors:
    print(f"Errors encountered: {len(errors)}")
    for err in errors:
        print(f"  - {err['repo']}: {err['error']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Load Commits

# CELL ********************

if all_commit_rows:
    df_commits = spark.createDataFrame(all_commit_rows, schema=COMMIT_SCHEMA)
    display(df_commits)

    merge_to_delta(
        df_commits,
        ENTITIES["gh_commits"]["write_path"],
        ENTITIES["gh_commits"]["merge_key"],
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
# Advance the `commits` watermark only if Step 4 succeeded.

# CELL ********************

new_watermark = extraction_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
set_watermark("commits", new_watermark)
print(f"Watermark 'commits' set to {new_watermark}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 6: Verification

# CELL ********************


repos_path = ENTITIES["gh_repositories"]["write_path"] 
if DeltaTable.isDeltaTable(spark, repos_path):
    df_verify_repos = spark.read.format("delta").load(repos_path)
    repo_count = df_verify_repos.count()

    print (f"gh_repositories: {repo_count} rows") 
    display(df_verify_repos.limit(5))
else:
    print("WARNING: gh_repositories table does not exist!")

commits_path=ENTITIES["gh_commits"]["write_path"]
if DeltaTable.isDeltaTable(spark, commits_path):
    df_verify_commits = spark.read.format("delta").load(commits_path)
    commit_count = df_verify_commits.count()
    print (f"gh_commits: {commit_count} rows")
    display(df_verify_commits.limit(5))
else:
    print("WARNING: gh_commits table does not exist!")
try:
    df_watermarks = spark.sql(f"""
    SELECT * FROM `{LH_WORKSPACE_NAME}`.{BRONZE_LH_NAME}.meta._load_watermarks
    """)
    print("\nLoad watermarks: ")
    display (df_watermarks)
except Exception:
    print("WARNING: meta._load_watermarks table does not exist!")
    
print("\nExtraction pipeline complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
