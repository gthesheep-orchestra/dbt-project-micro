{% macro motherduck_types_yaml() %}{% raw %}
# Canonical list of DuckDB/MotherDuck SQL data types.
# Source: duckdb_types() system function (duckdb==2.0.0)
# DO NOT edit manually — updated automatically via .github/workflows/sync-motherduck-types.yml

types:
- name: ARRAY
  example: '[1, 2, 3]::INTEGER[3]'
- name: BIGINT
  synonyms:
  - INT8
  - LONG
  example: 42::BIGINT
- name: BIGNUM
  synonyms:
  - VARINT
  example: 42::VARINT
- name: BIT
  synonyms:
  - BITSTRING
  example: '''101010''::BIT'
- name: BLOB
  synonyms:
  - BYTEA
  - BINARY
  - VARBINARY
  example: encode('hello')
- name: BOOLEAN
  synonyms:
  - BOOL
  - LOGICAL
  example: true::BOOLEAN
- name: DATE
  example: '''2024-01-01''::DATE'
- name: DECIMAL
  synonyms:
  - NUMERIC
  - DEC
  example: 3.14::DECIMAL(10, 2)
- name: DOUBLE
  synonyms:
  - FLOAT8
  example: 3.14::DOUBLE
- name: FLOAT
  synonyms:
  - FLOAT4
  - REAL
  example: 3.14::FLOAT
- name: GEOMETRY
  example: null
- name: HUGEINT
  synonyms:
  - INT128
  example: 42::HUGEINT
- name: INTEGER
  synonyms:
  - INT
  - INT4
  - SIGNED
  example: 42::INTEGER
- name: INTERVAL
  example: INTERVAL '1' YEAR
- name: LIST
  example: '[1, 2, 3]'
- name: MAP
  example: map(['a', 'b'], [1, 2])
- name: SMALLINT
  synonyms:
  - INT2
  - SHORT
  example: 1::SMALLINT
- name: STRUCT
  synonyms:
  - ROW
  example: '{''id'': 42, ''name'': ''hello''}'
- name: TIME
  example: '''12:34:56''::TIME'
- name: TIME WITH TIME ZONE
  synonyms:
  - TIMETZ
  example: '''12:34:56+00:00''::TIMETZ'
- name: TIMESTAMP
  synonyms:
  - DATETIME
  - TIMESTAMP_US
  example: '''2024-01-01 12:34:56''::TIMESTAMP'
- name: TIMESTAMP WITH TIME ZONE
  synonyms:
  - TIMESTAMPTZ
  example: '''2024-01-01 12:34:56+00:00''::TIMESTAMPTZ'
- name: TIMESTAMPTZ_NS
  storable: true
  example: TODO -- add example for TIMESTAMPTZ_NS
- name: TIMESTAMP_MS
  example: '''2024-01-01 12:34:56''::TIMESTAMP_MS'
- name: TIMESTAMP_NS
  example: '''2024-01-01 12:34:56''::TIMESTAMP_NS'
- name: TIMESTAMP_S
  example: '''2024-01-01 12:34:56''::TIMESTAMP_S'
- name: TIME_NS
  example: '''12:34:56.123456789''::TIME_NS'
- name: TINYINT
  synonyms:
  - INT1
  example: 1::TINYINT
- name: TUPLE
  storable: true
  example: TODO -- add example for TUPLE
- name: UBIGINT
  synonyms:
  - UINT64
  example: 42::UBIGINT
- name: UHUGEINT
  synonyms:
  - UINT128
  example: 42::UHUGEINT
- name: UINTEGER
  synonyms:
  - UINT32
  example: 42::UINTEGER
- name: UNION
  example: union_value(num := 42)
- name: USMALLINT
  synonyms:
  - UINT16
  example: 1::USMALLINT
- name: UTINYINT
  synonyms:
  - UINT8
  example: 1::UTINYINT
- name: UUID
  synonyms:
  - GUID
  example: gen_random_uuid()
- name: VARCHAR
  synonyms:
  - TEXT
  - STRING
  - CHAR
  - BPCHAR
  - NVARCHAR
  example: '''hello''::VARCHAR'
- name: VARIANT
  example: cast(42 as VARIANT)
{% endraw %}{% endmacro %}
