#!/usr/bin/env python3
"""
Scrapes https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types
and updates data/bigquery_types.yml with any new or removed types.

Targets the #data_type_list table directly, extracting the type name from the
first cell of each body row. This avoids false positives from section headings.

Exits with code 0 always. The CI workflow detects file changes via git diff.
"""

import re
import sys
import textwrap
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup

DOCS_URL = "https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types"
YAML_PATH = Path(__file__).parent.parent / "data" / "bigquery_types.yml"

# Known example expressions per type. New types discovered by the scraper will
# get a TODO placeholder so a human can fill in the real expression.
KNOWN_EXAMPLES: dict[str, str] = {
    "INT64":      "cast(42 as INT64)",
    "FLOAT64":    "cast(3.14 as FLOAT64)",
    "NUMERIC":    "cast(3.14159265358979323846 as NUMERIC)",
    "BIGNUMERIC": "cast(3.14159265358979323846264338327950288 as BIGNUMERIC)",
    "BOOL":       "cast(true as BOOL)",
    "STRING":     "cast('hello' as STRING)",
    "BYTES":      "cast(b'hello' as BYTES)",
    "DATE":       "cast('2024-01-01' as DATE)",
    "TIME":       "cast('12:34:56' as TIME)",
    "DATETIME":   "cast('2024-01-01T12:34:56' as DATETIME)",
    "TIMESTAMP":  "cast('2024-01-01T12:34:56Z' as TIMESTAMP)",
    "INTERVAL":   "cast('1-2 3 4:5:6' as INTERVAL)",
    "JSON":       "parse_json('{\"key\": \"value\"}')",
    "GEOGRAPHY":  "st_geogpoint(-122.4194, 37.7749)",
    "ARRAY":      "[1, 2, 3]",
    "STRUCT":     "struct('Alice' as name, 30 as age)",
    "RANGE":      "range(cast('2024-01-01' as DATE), cast('2024-12-31' as DATE))",
}


def fetch_types_from_docs() -> list[str]:
    resp = requests.get(DOCS_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # The canonical type list lives in a table with id="data_type_list".
    # The anchor element may be a <div>, <section>, or the <table> itself,
    # so we find the nearest following <table> if needed.
    anchor = soup.find(id="data_type_list")
    if anchor is None:
        print("ERROR: Could not find #data_type_list on the page — the page structure may have changed.")
        sys.exit(2)

    table = anchor if anchor.name == "table" else anchor.find_next("table")
    if table is None:
        print("ERROR: Found #data_type_list anchor but no table follows it — the page structure may have changed.")
        sys.exit(2)

    types: list[str] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue  # skip <th> header rows

        # Type name is in the first cell, typically wrapped in a <code> tag.
        first_cell = cells[0]
        code_tag = first_cell.find("code")
        raw = (code_tag or first_cell).get_text(strip=True)

        # Normalise to uppercase and skip anything that doesn't look like an
        # identifier (e.g. empty cells or cells containing only punctuation).
        name = raw.upper()
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
            types.append(name)

    return types


def load_yaml() -> dict:
    with YAML_PATH.open() as f:
        return yaml.safe_load(f)


def write_yaml(data: dict) -> None:
    header = textwrap.dedent(f"""\
        # Canonical list of BigQuery GoogleSQL data types.
        # Source: {DOCS_URL}
        # DO NOT edit manually — updated automatically via .github/workflows/sync-bigquery-types.yml

    """)
    with YAML_PATH.open("w") as f:
        f.write(header)
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def main() -> None:
    print(f"Fetching {DOCS_URL} ...")
    scraped = fetch_types_from_docs()
    if not scraped:
        print("ERROR: No types found in #data_type_list — the page structure may have changed.")
        sys.exit(2)

    print(f"Found {len(scraped)} types: {', '.join(scraped)}")

    current_data = load_yaml()
    current_types = {t["name"] for t in current_data["types"]}
    scraped_set = set(scraped)

    added = scraped_set - current_types
    removed = current_types - scraped_set

    if not added and not removed:
        print("No changes detected.")
        sys.exit(0)

    if added:
        print(f"New types: {', '.join(sorted(added))}")
    if removed:
        print(f"Removed types: {', '.join(sorted(removed))}")

    # Rebuild the list: keep existing entries, append new ones, drop removed ones.
    existing = {t["name"]: t for t in current_data["types"]}
    new_types = []
    for name in scraped:  # preserve page order
        if name in existing:
            new_types.append(existing[name])
        else:
            new_types.append({
                "name": name,
                "example": KNOWN_EXAMPLES.get(name, f"TODO -- add example for {name}"),
            })

    current_data["types"] = new_types
    write_yaml(current_data)
    print(f"Updated {YAML_PATH}")
    sys.exit(0)  # CI detects changes via git diff, not exit code


if __name__ == "__main__":
    main()
