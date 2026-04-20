#!/usr/bin/env python3
"""
Scrapes https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes
and updates data/databricks_types.yml with any new or removed types.

The page contains a table where each cell holds a link of the form:
  <a href="/aws/en/sql/language-manual/data-types/{name}-type">
We extract type names from those href patterns.

Exits 0 = no changes, 1 = file updated (CI opens PR), 2 = fetch error.
"""

import re
import sys
import textwrap
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup

DOCS_URL  = "https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes"
YAML_PATH = Path(__file__).parent.parent / "data" / "databricks_types.yml"

# Known example expressions for all current types.
# New types discovered by the scraper will get a TODO placeholder.
KNOWN_EXAMPLES: dict[str, str | None] = {
    "TINYINT":      "cast(1 as TINYINT)",
    "SMALLINT":     "cast(1 as SMALLINT)",
    "INT":          "cast(42 as INT)",
    "BIGINT":       "cast(42 as BIGINT)",
    "DECIMAL":      "cast(3.14 as DECIMAL(10, 2))",
    "FLOAT":        "cast(3.14 as FLOAT)",
    "DOUBLE":       "cast(3.14 as DOUBLE)",
    "BOOLEAN":      "cast(true as BOOLEAN)",
    "STRING":       "cast('hello' as STRING)",
    "BINARY":       "cast('hello' as BINARY)",
    "DATE":         "cast('2024-01-01' as DATE)",
    "TIMESTAMP":    "cast('2024-01-01 12:34:56' as TIMESTAMP)",
    "TIMESTAMP_NTZ": "cast('2024-01-01 12:34:56' as TIMESTAMP_NTZ)",
    "INTERVAL":     "INTERVAL '1' YEAR",
    "ARRAY":        "array(1, 2, 3)",
    "MAP":          "map('a', 1, 'b', 2)",
    "STRUCT":       "struct(42 as id, 'hello' as name)",
    "VARIANT":      "parse_json('{\"key\": \"value\"}')",
    "OBJECT":       "cast(parse_json('{\"key\": \"value\"}') as OBJECT)",
    "GEOMETRY":     "st_geomfromwkt('POINT(-122.4194 37.7749)')",
    "GEOGRAPHY":    "st_geogpoint(-122.4194, 37.7749)",
    "VOID":         None,   # not a storable column type
}

NON_STORABLE: set[str] = {"VOID"}

# href pattern: /aws/en/sql/language-manual/data-types/{slug}-type
_TYPE_HREF_RE = re.compile(r"/sql/language-manual/data-types/([a-z0-9_]+)-type$")


def fetch_types_from_docs() -> list[str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(DOCS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    seen: set[str] = set()
    types: list[str] = []

    for a in soup.find_all("a", href=True):
        m = _TYPE_HREF_RE.search(a["href"])
        if not m:
            continue
        # Convert slug to canonical name: timestamp_ntz → TIMESTAMP_NTZ
        name = m.group(1).upper().replace("-", "_")
        # Strip trailing suffixes that appear in some slugs (e.g. 'null' for VOID)
        # Map known slug→name overrides
        SLUG_OVERRIDES = {"NULL": "VOID"}
        name = SLUG_OVERRIDES.get(name, name)
        if name not in seen:
            seen.add(name)
            types.append(name)

    return types


def load_yaml() -> dict:
    with YAML_PATH.open() as f:
        return yaml.safe_load(f)


def write_yaml(data: dict) -> None:
    header = textwrap.dedent(f"""\
        # Canonical list of Databricks SQL data types.
        # Source: {DOCS_URL}
        # DO NOT edit manually — updated automatically via .github/workflows/sync-databricks-types.yml

    """)
    with YAML_PATH.open("w") as f:
        f.write(header)
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def main() -> None:
    print(f"Fetching {DOCS_URL} ...")
    try:
        scraped = fetch_types_from_docs()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(2)

    if not scraped:
        print("ERROR: No types found — the page structure may have changed.")
        sys.exit(2)

    print(f"Found {len(scraped)} types: {', '.join(scraped)}")

    current_data  = load_yaml()
    current_types = {t["name"]: t for t in current_data["types"]}
    scraped_set   = set(scraped)

    added   = scraped_set - current_types.keys()
    removed = current_types.keys() - scraped_set

    if not added and not removed:
        print("No changes detected.")
        sys.exit(0)

    if added:
        print(f"New types: {', '.join(sorted(added))}")
    if removed:
        print(f"Removed types: {', '.join(sorted(removed))}")

    new_types = []
    for name in scraped:          # preserve page order
        if name in current_types:
            new_types.append(current_types[name])
        else:
            example = KNOWN_EXAMPLES.get(name, f"TODO -- add example for {name}")
            new_types.append({
                "name":     name,
                "storable": name not in NON_STORABLE,
                "example":  example,
            })

    current_data["types"] = new_types
    write_yaml(current_data)
    print(f"Updated {YAML_PATH}")
    sys.exit(1)   # signals CI: file changed, open a PR


if __name__ == "__main__":
    main()
