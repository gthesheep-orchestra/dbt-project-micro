#!/usr/bin/env python3
"""
Reads the canonical BigQuery GoogleSQL data types from the official
google-cloud-bigquery Python library (StandardSqlTypeNames enum) and updates
data/bigquery_types.yml with any new or removed types.

Using the library instead of scraping the docs page means:
  - No fragile HTML parsing
  - Types are always accurate (Google maintains the enum)
  - New types are detected by upgrading the package in CI

Exits with code 0 always. The CI workflow detects file changes via git diff.
"""

import sys
import textwrap
from pathlib import Path

import yaml
from google.cloud.bigquery.enums import StandardSqlTypeNames

DOCS_URL = "https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types"
YAML_PATH = Path(__file__).parent.parent / "data" / "bigquery_types.yml"

# Internal / non-user-facing values present in the enum but not real SQL types.
_SKIP = {"TYPE_KIND_UNSPECIFIED", "FOREIGN"}

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


def fetch_types_from_library() -> list[str]:
    types = sorted(
        t.value for t in StandardSqlTypeNames if t.value not in _SKIP
    )
    if not types:
        print("ERROR: StandardSqlTypeNames enum is empty — check the library version.")
        sys.exit(2)
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
    import importlib.metadata
    version = importlib.metadata.version("google-cloud-bigquery")
    print(f"google-cloud-bigquery=={version}")

    scraped = fetch_types_from_library()
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

    existing = {t["name"]: t for t in current_data["types"]}
    new_types = []
    for name in scraped:  # preserve sorted order
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
    sys.exit(0)


if __name__ == "__main__":
    main()
