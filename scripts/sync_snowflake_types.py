#!/usr/bin/env python3
"""
Scrapes https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
and updates data/snowflake_types.yml with any new or removed types.

The summary page has a Category | Type | Notes table. We use the Notes column
to distinguish canonical types from synonym rows: if Notes says "Synonymous
with X", those names are folded in as synonyms of X rather than new types.

Exits with code 0 always. The CI workflow detects file changes via git diff.
"""

import re
import sys
import textwrap
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup

DOCS_URL  = "https://docs.snowflake.com/en/sql-reference/intro-summary-data-types"
YAML_PATH = Path(__file__).parent.parent / "data" / "snowflake_types.yml"

KNOWN_EXAMPLES: dict[str, str | None] = {
    "NUMBER":        "cast(42 as NUMBER)",
    "FLOAT":         "cast(3.14 as FLOAT)",
    "DECFLOAT":      "'3.14159265358979323846264338327950288'::DECFLOAT",
    "VARCHAR":       "cast('hello' as VARCHAR)",
    "BINARY":        "to_binary('68656c6c6f', 'HEX')",
    "BOOLEAN":       "cast(true as BOOLEAN)",
    "DATE":          "cast('2024-01-01' as DATE)",
    "TIME":          "cast('12:34:56' as TIME)",
    "TIMESTAMP":     "cast('2024-01-01 12:34:56' as TIMESTAMP)",
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
    "FILE":          None,
}

NON_STORABLE: set[str] = {"FILE"}
SKIP:         set[str] = {"NOT APPLICABLE"}

# Matches "Synonymous with TYPENAME" or "Synonymous with TYPENAME." in Notes.
_SYNONYMOUS_RE = re.compile(r"synonymous with\s+([A-Z_]+)", re.IGNORECASE)


def _parse_names(cell_text: str) -> list[str]:
    """Split a Type cell on commas, returning normalised uppercase names."""
    return [p.strip().upper() for p in cell_text.split(",") if p.strip()]


def fetch_types_from_docs() -> list[dict]:
    resp = requests.get(DOCS_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table")
    if not table:
        raise ValueError("Could not find the summary table on the page.")

    # canonical_name -> {"canonical": str, "synonyms": list[str]}
    type_map:   dict[str, dict] = {}
    type_order: list[str]       = []   # preserves page order

    for row in table.find_all("tr")[1:]:   # skip header row
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        type_text  = cells[1].get_text(separator=", ", strip=True)
        notes_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""

        names = _parse_names(type_text)
        if not names or names[0] in SKIP:
            continue

        # If Notes say "Synonymous with X", fold all names in this row into
        # X's synonym list rather than treating them as a new canonical type.
        m = _SYNONYMOUS_RE.search(notes_text)
        if m:
            ref = m.group(1).upper()
            if ref in type_map:
                existing = type_map[ref]["synonyms"]
                for name in names:
                    if name not in existing:
                        existing.append(name)
            # If ref not yet seen, silently skip — shouldn't happen given page order.
            continue

        # New canonical type: first name is canonical, rest are synonyms.
        canonical = names[0]
        synonyms  = names[1:]

        if canonical not in type_map:
            type_map[canonical] = {"canonical": canonical, "synonyms": synonyms}
            type_order.append(canonical)
        else:
            # ARRAY and OBJECT each appear in both semi-structured and structured
            # sections — merge any new synonyms on the second occurrence.
            for name in synonyms:
                if name not in type_map[canonical]["synonyms"]:
                    type_map[canonical]["synonyms"].append(name)

    return [type_map[name] for name in type_order]


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
        # 'example' is the SQL expression used in the dbt model; null excludes the column.

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

    scraped_names = [t["canonical"] for t in scraped]
    print(f"Found {len(scraped_names)} types: {', '.join(scraped_names)}")

    current_data  = load_yaml()
    current_types = {t["name"]: t for t in current_data["types"]}
    scraped_set   = set(scraped_names)

    added   = scraped_set - current_types.keys()
    removed = current_types.keys() - scraped_set

    synonym_changed = any(
        set(t["synonyms"]) != set(current_types[t["canonical"]].get("synonyms", []))
        for t in scraped
        if t["canonical"] in current_types
    )

    if not added and not removed and not synonym_changed:
        print("No changes detected.")
        sys.exit(0)

    if added:
        print(f"New types: {', '.join(sorted(added))}")
    if removed:
        print(f"Removed types: {', '.join(sorted(removed))}")

    new_types = []
    for t in scraped:
        name = t["canonical"]
        if name in current_types:
            entry = dict(current_types[name])
            entry["synonyms"] = t["synonyms"]
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


if __name__ == "__main__":
    main()
