#!/usr/bin/env python3
"""
Agentic CI repair agent.

Fetches failed job logs for a workflow run, uses Claude to analyse failures and
write targeted fixes, then exits 0 if files were modified (signals the calling
workflow to open a PR).

Required env vars:
  ANTHROPIC_API_KEY   — Claude API key
  GITHUB_TOKEN        — for fetching job logs via GitHub API
  WORKFLOW_RUN_ID     — numeric ID of the failed workflow run
  GITHUB_REPOSITORY   — "owner/repo"
  GITHUB_WORKSPACE    — path to checked-out repo (default: cwd)
  RUNNER_TEMP         — temp dir for metadata files (default: /tmp)
"""
import json
import os
import sys
import textwrap
from pathlib import Path

import anthropic
import requests

# ── Config ─────────────────────────────────────────────────────────────────────

REPO          = os.environ["GITHUB_REPOSITORY"]
RUN_ID        = os.environ["WORKFLOW_RUN_ID"]
WORKFLOW_NAME = os.environ.get("WORKFLOW_NAME", "unknown workflow")
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]
WORKSPACE     = Path(os.environ.get("GITHUB_WORKSPACE", ".")).resolve()
META_DIR      = Path(os.environ.get("RUNNER_TEMP", "/tmp"))
MODEL         = "claude-sonnet-4-6"
MAX_ITER      = 12
LOG_CAP       = 10_000  # max chars retained per failed job log (tail)

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# ── GitHub helpers ──────────────────────────────────────────────────────────────

def gh_get(path: str, **kwargs):
    url = f"https://api.github.com{path}"
    r = requests.get(url, headers=GH_HEADERS, allow_redirects=True, **kwargs)
    ct = r.headers.get("Content-Type", "")
    return r.json() if "json" in ct else r.text


def fetch_failed_logs() -> str:
    jobs = gh_get(f"/repos/{REPO}/actions/runs/{RUN_ID}/jobs")
    parts = []
    for job in jobs.get("jobs", []):
        if job["conclusion"] not in ("failure", "timed_out"):
            continue
        jid   = job["id"]
        jname = job["name"]
        text  = gh_get(f"/repos/{REPO}/actions/jobs/{jid}/logs")
        if isinstance(text, dict):
            text = json.dumps(text)
        text = text[-LOG_CAP:] if len(text) > LOG_CAP else text
        parts.append(f"=== Job: {jname} ===\n{text}")
    return "\n\n".join(parts) or "No failed jobs found."

# ── File tools ──────────────────────────────────────────────────────────────────

def _safe_path(path: str) -> Path:
    full = (WORKSPACE / path).resolve()
    if not str(full).startswith(str(WORKSPACE)):
        raise ValueError(f"Path escape attempt blocked: {path}")
    return full


def tool_read_file(path: str) -> str:
    p = _safe_path(path)
    if not p.exists():
        return f"File not found: {path}"
    return p.read_text(encoding="utf-8", errors="replace")


def tool_write_file(path: str, content: str) -> str:
    p = _safe_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return f"Wrote {path} ({len(content)} chars)"


def tool_list_files(pattern: str) -> str:
    import glob
    files = sorted(glob.glob(pattern, root_dir=str(WORKSPACE), recursive=True))
    return "\n".join(files) if files else "(no matches)"

# ── Tool definitions ────────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "read_file",
        "description": "Read a file from the repository. Path is relative to the repo root.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Repo-relative file path"},
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": (
            "Overwrite a file in the repository with new content. "
            "Path is relative to the repo root. Creates parent directories as needed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path":    {"type": "string"},
                "content": {"type": "string"},
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "list_files",
        "description": (
            "List repo files matching a glob pattern. "
            "Use ** for recursive matching. Path relative to repo root."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern"},
            },
            "required": ["pattern"],
        },
    },
    {
        "name": "finish",
        "description": (
            "Signal completion. Call this once when you have either applied all fixes "
            "or determined the failure cannot be resolved from code alone."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "changed": {
                    "type": "boolean",
                    "description": "True if any files were modified; false otherwise.",
                },
                "pr_title": {
                    "type": "string",
                    "description": "Short PR title (required when changed=true).",
                },
                "pr_body": {
                    "type": "string",
                    "description": "Markdown PR description (required when changed=true).",
                },
                "summary": {
                    "type": "string",
                    "description": "One-paragraph explanation of what was found and done.",
                },
            },
            "required": ["changed", "summary"],
        },
    },
]

SYSTEM = textwrap.dedent("""
    You are an autonomous CI repair agent for a dbt project that validates native SQL
    data types across four warehouses: BigQuery, Snowflake, Databricks, and MotherDuck (DuckDB).

    Repository layout:
      models/all_types.sql                    dbt model — SELECTs every native type via macros
      macros/<warehouse>_type_examples.sql    Jinja macros that build the SELECT list
      data/<warehouse>_types.yml              Canonical YAML: name + example SQL expression per type
      scripts/sync_<warehouse>_types.py       Weekly scrapers that update the YAML files
      .github/workflows/dbt-run.yml           Runs dbt against all warehouses on push/PR
      .github/workflows/sync-warehouse-types.yml  Monday scrape + auto-PR workflow
      pyproject.toml                          Python package deps per warehouse

    Repair workflow:
    1. Read the failure logs to identify the root cause.
    2. Use read_file / list_files to inspect the relevant source files.
    3. Use write_file to apply the minimal targeted fix (edit only what is broken).
    4. Call finish(changed=true, pr_title=..., pr_body=..., summary=...).

    Call finish(changed=false, summary=...) if the failure is:
    - Transient (network timeout, rate limit, flaky external service)
    - Requires secret rotation or external action you cannot perform
    - Caused by something genuinely ambiguous where guessing would be worse than leaving it

    Common fixable failures in this repo:
    - Scraper regex/selector broken because a docs page HTML structure changed
    - New SQL type not yet in a YAML file — add it with a correct example expression
    - dbt macro references a type name that no longer matches the YAML (name drift)
    - Python dependency version conflict in pyproject.toml
    - Malformed YAML syntax in a workflow or data file

    Only write code you are confident is correct. Never guess or speculate.
    Be surgical: change only lines that are broken, leave everything else intact.
""").strip()

# ── Agent loop ──────────────────────────────────────────────────────────────────

def run_agent(logs: str) -> dict:
    client = anthropic.Anthropic()
    messages = [
        {
            "role": "user",
            "content": (
                f"Workflow **{WORKFLOW_NAME}** (run {RUN_ID}) failed. "
                "Analyse the logs below, read the relevant files, fix the root cause, "
                "then call `finish`.\n\n"
                f"<logs>\n{logs}\n</logs>"
            ),
        }
    ]

    result: dict = {
        "changed": False,
        "summary": "Agent did not call finish — max iterations reached.",
        "pr_title": "",
        "pr_body": "",
    }

    for i in range(MAX_ITER):
        response = client.messages.create(
            model=MODEL,
            max_tokens=8096,
            system=SYSTEM,
            tools=TOOLS,
            messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            print("Agent stopped (end_turn) without calling finish.")
            break

        tool_results = []
        done = False

        for block in response.content:
            if block.type != "tool_use":
                continue

            name   = block.name
            inputs = block.input

            if name == "finish":
                result = inputs
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": "Acknowledged.",
                })
                done = True
                break

            elif name == "read_file":
                output = tool_read_file(inputs["path"])
            elif name == "write_file":
                output = tool_write_file(inputs["path"], inputs["content"])
            elif name == "list_files":
                output = tool_list_files(inputs["pattern"])
            else:
                output = f"Unknown tool: {name}"

            preview = output[:200].replace("\n", "↵")
            print(f"[{i+1}] {name}({inputs}) → {preview}")

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": output,
            })

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

        if done:
            break

    return result

# ── Metadata output ─────────────────────────────────────────────────────────────

def write_metadata(result: dict) -> None:
    meta = {
        "changed":  result.get("changed", False),
        "pr_title": result.get("pr_title", ""),
        "summary":  result.get("summary", ""),
    }
    (META_DIR / "agent_fix_meta.json").write_text(json.dumps(meta, indent=2))

    body = result.get("pr_body") or ""
    if not body:
        run_url = f"https://github.com/{REPO}/actions/runs/{RUN_ID}"
        body = (
            f"Automated fix for failed workflow run [{RUN_ID}]({run_url}).\n\n"
            f"**Summary:** {result.get('summary', '')}\n\n"
            "_Opened automatically by the `agentic-fix` workflow._"
        )
    (META_DIR / "agent_fix_pr_body.md").write_text(body)

# ── Entrypoint ──────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Fetching logs for run {RUN_ID} ({WORKFLOW_NAME}) in {REPO} ...")
    logs = fetch_failed_logs()
    print(f"Fetched {len(logs):,} chars of failure logs.\n")

    result = run_agent(logs)
    write_metadata(result)

    print(f"\nSummary: {result.get('summary', '')}")
    if result.get("changed"):
        print("Files modified — exiting 0 to trigger PR creation.")
        sys.exit(0)
    else:
        print("No changes made — exiting 1.")
        sys.exit(1)


if __name__ == "__main__":
    main()
