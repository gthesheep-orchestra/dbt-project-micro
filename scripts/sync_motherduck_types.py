#!/usr/bin/env python3
"""
Refreshes data/motherduck_types.yml from DuckDB's built-in duckdb_types() catalog.

MotherDuck is DuckDB in the cloud; its type system is identical to the open-source
DuckDB engine. We query a local in-memory DuckDB connection — no MotherDuck
credentials needed.

The script detects additions and removals against the current YAML and rewrites it
if anything changed, preserving existing 'example' and 'synonyms' values.

Exits 0 = no changes, 1 = file updated (CI opens PR), 2 = query error.
"""

import sys
import textwrap
from pathlib import Path

import duckdb
import yaml

DOCS_URL  = "https://duckdb.org/docs/sql/data_types/overview"
YAML_PATH = Path(__file__).parent.parent / "data" / "motherduck_types.yml"

# Known example expressions; new types discovered get a TODO placeholder.
KNOWN_EXAMPLES: dict[str, str | None] = {
    "TINYINT":                  "1::TINYINT",
    "SMALLINT":                 "1::SMALLINT",
    "INTEGER":                  "42::INTEGER",
    "BIGINT":                   "42::BIGINT",
    "HUGEINT":                  "42::HUGEINT",
    "UTINYINT":                 "1::UTINYINT",
    "USMALLINT":                "1::USMALLINT",
    "UINTEGER":                 "42::UINTEGER",
    "UBIGINT":                  "42::UBIGINT",
    "UHUGEINT":                 "42::UHUGEINT",
    "DECIMAL":                  "3.14::DECIMAL(10, 2)",
    "FLOAT":                    "3.14::FLOAT",
    "DOUBLE":                   "3.14::DOUBLE",
    "BIGNUM":                   "42::VARINT",
    "BOOLEAN":                  "true::BOOLEAN",
    "VARCHAR":                  "'hello'::VARCHAR",
    "BLOB":                     "encode('hello')",
    "BIT":                      "'101010'::BIT",
    "UUID":                     "gen_random_uuid()",
    "DATE":                     "'2024-01-01'::DATE",
    "TIME":                     "'12:34:56'::TIME",
    "TIME WITH TIME ZONE":      "'12:34:56+00:00'::TIMETZ",
    "TIME_NS":                  "'12:34:56.123456789'::TIME_NS",
    "TIMESTAMP":                "'2024-01-01 12:34:56'::TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "'2024-01-01 12:34:56+00:00'::TIMESTAMPTZ",
    "TIMESTAMP_S":              "'2024-01-01 12:34:56'::TIMESTAMP_S",
    "TIMESTAMP_MS":             "'2024-01-01 12:34:56'::TIMESTAMP_MS",
    "TIMESTAMP_NS":             "'2024-01-01 12:34:56'::TIMESTAMP_NS",
    "INTERVAL":                 "INTERVAL '1' YEAR",
    "LIST":                     "[1, 2, 3]",
    "ARRAY":                    "[1, 2, 3]::INTEGER[3]",
    "STRUCT":                   "{'id': 42, 'name': 'hello'}",
    "MAP":                      "map(['a', 'b'], [1, 2])",
    "UNION":                    "union_value(num := 42)",
    "VARIANT":                  "to_variant(42)",
    "GEOMETRY":                 None,  # requires LOAD spatial
}

# Types excluded from the dbt model even if storable
NON_STORABLE: set[str] = set()


def fetch_types_from_catalog() -> list[str]:
    """Query a local in-memory DuckDB instance for its canonical type list."""
    conn = duckdb.connect()
    rows = conn.execute("""
        SELECT DISTINCT logical_type
        FROM duckdb_types()
        WHERE logical_type IS NOT NULL
          AND logical_type NOT IN ('NULL', 'TYPE', 'ENUM')
        ORDER BY logical_type
    """).fetchall()
    conn.close()
    return [r[0] for r in rows]


def load_yaml() -> dict:
    with YAML_PATH.open() as f:
        return yaml.safe_load(f)


def write_yaml(data: dict) -> None:
    import duckdb as _duckdb
    header = textwrap.dedent(f"""\
        # Canonical list of DuckDB/MotherDuck SQL data types.
        # Source: duckdb_types() system function (duckdb=={_duckdb.__version__})
        # DO NOT edit manually — updated automatically via .github/workflows/sync-motherduck-types.yml

    """)
    with YAML_PATH.open("w") as f:
        f.write(header)
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def main() -> None:
    print(f"Querying local DuckDB catalog (duckdb=={duckdb.__version__}) ...")
    try:
        scraped = fetch_types_from_catalog()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(2)

    if not scraped:
        print("ERROR: No types returned — unexpected empty result.")
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
    for name in scraped:          # catalog order (alphabetical)
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
    sys.exit(1)


if __name__ == "__main__":
    main()
