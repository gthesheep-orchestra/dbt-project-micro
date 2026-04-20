#!/usr/bin/env python3
"""
Scrapes https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
and updates data/snowflake_types.yml with any new or removed types.

The summary page contains an HTML table with columns: Category | Type | Notes.
Each "Type" cell lists the canonical name, optionally followed by synonyms
separated by commas (e.g. "DECIMAL, NUMERIC" or "INT, INTEGER, BIGINT, ...").
We take the first token as the canonical name.

Exits with code 1 if the file was changed (signals CI to open a PR),
         code 0 if nothing changed,
         code 2 if the scrape failed.
"""

import re
import sys
import textwrap
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup

DOCS_URL = "https://docs.snowflake.com/en/sql-reference/intro-summary-data-types"
YAML_PATH = Path(__file__).parent.parent / "data" / "snowflake_types.yml"

# Canonical example expressions per type.
# New types discovered by the scraper get a TODO placeholder.
KNOWN_EXAMPLES: dict[str, str | None] = {
    "NUMBER":        "cast(42 as NUMBER)",
    "FLOAT":         "cast(3.14 as FLOAT)",
    "DECFLOAT":      "'3.14159265358979323846264338327950288'::DECFLOAT",
    "VARCHAR":       "cast('hello' as VARCHAR)",
    "BINARY":        "to_binary('68656c6c6f', 'HEX')",
    "BOOLEAN":       "cast(true as BOOLEAN)",
    "DATE":          "cast('2024-01-01' as DATE)",
    "TIME":          "cast('12:34:56' as TIME)",
    "TIMESTAMP_LTZ": "cast('2024-01-01 12:34:56' as TIMESTAMP_LTZ)",
    "TIMESTAMP_NTZ": "cast('2024-01-01 12:34:56' as TIMESTAMP_NTZ)",
    "TIMESTAMP_TZ":  "cast('2024-01-01 12:34:56 +00:00' as TIMESTAMP_TZ)",
    "VARIANT":       "parse_json('{\"key\": \"value\"}')",
    "OBJECT":        "parse_json('{\"key\": \"value\"}')::OBJECT",
    "ARRAY":         "parse_json('[1, 2, 3]')::ARRAY",
    "MAP":           "{'a': 1, 'b': 2}::MAP(VARCHAR, NUMBER)",
    "GEOGRAPHY":     "to_geography('POINT(-122.4194 37.7749)')",
    "GEOMETRY":      "to_geometry('POINT(-122.4194 37.7749)')",
    "UUID":          "uuid_string()::UUID",
    "VECTOR":        "[1.0, 2.0, 3.0]::VECTOR(FLOAT, 3)",
    # FILE can't be expressed as a SELECT literal
    "FILE":          None,
}

# Types that cannot be stored as a regular table column
NON_STORABLE = {"FILE"}

# Types that are purely user-defined or meta-categories — skip entirely
SKIP = {"NOT APPLICABLE"}


def parse_type_cell(cell_text: str) -> tuple[str, list[str]]:
    """Return (canonical_name, [synonyms]) from a Type cell."""
    # Split on comma, strip whitespace
    parts = [p.strip() for p in cell_text.split(",") if p.strip()]
    if not parts:
        return "", []
    canonical = parts[0].upper()
    synonyms = [p.upper() for p in parts[1:]]
    return canonical, synonyms


def fetch_types_from_docs() -> list[dict]:
    resp = requests.get(DOCS_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the summary table — it's the only table on the page
    table = soup.find("table")
    if not table:
        raise ValueError("Could not find the summary table on the page.")

    seen: set[str] = set()
    results: list[dict] = []

    for row in table.find_all("tr")[1:]:  # skip header row
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        type_text = cells[1].get_text(separator=", ", strip=True)
        canonical, synonyms = parse_type_cell(type_text)

        if not canonical or canonical in SKIP or canonical in seen:
            continue

        # Deduplicate ARRAY and OBJECT that appear in both semi-structured
        # and structured sections — keep only the first occurrence.
        seen.add(canonical)
        results.append({"canonical": canonical, "synonyms": synonyms})

    return results


def load_yaml() -> dict:
    with YAML_PATH.open() as f:
        return yaml.safe_load(f)


def write_yaml(data: dict) -> None:
    header = textwrap.dedent(f"""\
        # Canonical list of Snowflake SQL data types.
        # Source: {DOCS_URL}
        # DO NOT edit manually — updated automatically via .github/workflows/sync-snowflake-types.yml
        #
        # 'synonyms' lists alternative names accepted by Snowflake for the same type.
        # 'storable' is false for types that can't be used as a table column (e.g. FILE).
        # 'example' is the SQL expression used in the dbt model; set to null to exclude the column.

    """)
    with YAML_PATH.open("w") as f:
        f.write(header)
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def main() -> None:
    print(f"Fetching {DOCS_URL} ...")
    scraped = fetch_types_from_docs()
    if not scraped:
        print("ERROR: No types found — the page structure may have changed.")
        sys.exit(2)

    scraped_names = [t["canonical"] for t in scraped]
    print(f"Found {len(scraped_names)} types: {', '.join(scraped_names)}")

    current_data = load_yaml()
    current_types = {t["name"]: t for t in current_data["types"]}
    scraped_set = set(scraped_names)

    added = scraped_set - current_types.keys()
    removed = current_types.keys() - scraped_set

    if not added and not removed:
        # Also check for synonym changes
        synonym_changed = any(
            set(t["synonyms"]) != set(current_types[t["canonical"]].get("synonyms", []))
            for t in scraped
            if t["canonical"] in current_types
        )
        if not synonym_changed:
            print("No changes detected.")
            sys.exit(0)

    if added:
        print(f"New types: {', '.join(sorted(added))}")
    if removed:
        print(f"Removed types: {', '.join(sorted(removed))}")

    # Rebuild in page order: update existing entries, append new, drop removed
    new_types = []
    for t in scraped:
        name = t["canonical"]
        if name in current_types:
            entry = dict(current_types[name])
            entry["synonyms"] = t["synonyms"]  # refresh synonyms
            new_types.append(entry)
        else:
            example = KNOWN_EXAMPLES.get(name, f"TODO -- add example for {name}")
            new_types.append({
                "name":     name,
                "synonyms": t["synonyms"],
                "storable": name not in NON_STORABLE,
                "example":  example,
            })

    current_data["types"] = new_types
    write_yaml(current_data)
    print(f"Updated {YAML_PATH}")
    sys.exit(1)  # signals CI: file changed, open a PR


if __name__ == "__main__":
    main()
