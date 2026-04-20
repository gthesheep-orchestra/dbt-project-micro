# warehouse-types

A minimal dbt project that creates a single-row table containing one column per
native data type for your chosen warehouse.

Supported warehouses: **BigQuery**, **Snowflake**, **Databricks**, **MotherDuck**

---

## TLDR (Minimal Working Example)

```bash
pip install ".[motherduck]"
export MOTHERDUCK_TOKEN=your_token
dbt run --target motherduck --profiles-dir .
```

## Running locally

### 1. Install

```bash
# Install for your warehouse
pip install ".[bigquery]"
pip install ".[snowflake]"
pip install ".[databricks]"
pip install ".[motherduck]"

# Or everything at once
pip install ".[all]"

# Sync scripts only (no dbt adapter)
pip install ".[sync]"
```

### 2. Configure credentials

Credentials are read from environment variables so nothing secret lives in the repo.

**BigQuery**
```bash
export BQ_PROJECT=your_gcp_project
export BQ_DATASET=your_dataset
export BQ_LOCATION=your_bigquery_location

# Then authenticate with the gcloud CLI
gcloud auth application-default login
```

**Snowflake**
```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=your_warehouse
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_SCHEMA=your_schema
export SNOWFLAKE_ROLE=your_role
```

**Databricks**
```bash
export DATABRICKS_HOST=your_workspace.your_host.net   # or .cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
export DATABRICKS_TOKEN=your_personal_access_token
export DATABRICKS_CATALOG=your_catalog
export DATABRICKS_SCHEMA=your_schema
```

**MotherDuck**
```bash
export MOTHERDUCK_TOKEN=your_motherduck_token   # from app.motherduck.com → settings
export MOTHERDUCK_DATABASE=my_db
```

> **Note:** MotherDuck uses `dbt-duckdb` as its adapter. The `--target motherduck`
> flag selects it by *target name* rather than adapter type, because both a local
> DuckDB target and MotherDuck share `target.type == 'duckdb'`.

### 3. Run

Pass `--target` to choose the warehouse. Only that warehouse's models will run;
the other warehouse's models are automatically disabled.

```bash
dbt run --target <warehouse name>  --profiles-dir .
```

The default target (when `--target` is omitted) is `bigquery`. Change the
`target:` key in `profiles.yml` to make Snowflake the default instead.

---

## Project structure

```
models/
  all_types.sql ← Single model for any warehouse type

data/
  bigquery_types.yml   ← canonical type list + example SQL expressions
  snowflake_types.yml  ← canonical type list + example SQL expressions

macros/
  bigquery_type_examples.sql   ← reads bigquery_types.yml, generates SELECT
  snowflake_type_examples.sql  ← reads snowflake_types.yml, generates SELECT

scripts/
  sync_bigquery_types.py   ← scrapes BigQuery docs, updates bigquery_types.yml
  sync_snowflake_types.py  ← scrapes Snowflake docs, updates snowflake_types.yml
  requirements.txt

.github/workflows/
  dbt-run.yml              ← runs dbt on push/PR (both warehouses) or manually (one)
  sync-bigquery-types.yml  ← weekly: scrape BigQuery docs, open PR if changed
  sync-snowflake-types.yml ← weekly: scrape Snowflake docs, open PR if changed
```

---

## CI / GitHub Actions

### `dbt-run.yml`

Runs on every push to `main` and every pull request. Runs both warehouses in
parallel using a matrix strategy.

Can also be triggered manually via **Actions → dbt run → Run workflow**, where
you can choose a single warehouse from a dropdown.

Requires the following GitHub **secrets** and **variables**:

| Kind     | Name                  | Warehouse  |
|----------|-----------------------|------------|
| Secret   | `GCP_CREDENTIALS_JSON`| BigQuery   |
| Variable | `BQ_PROJECT`          | BigQuery   |
| Variable | `BQ_DATASET`          | BigQuery   |
| Variable | `BQ_LOCATION`         | BigQuery   |
| Secret   | `SNOWFLAKE_PASSWORD`  | Snowflake  |
| Variable | `SNOWFLAKE_ACCOUNT`   | Snowflake  |
| Variable | `SNOWFLAKE_USER`      | Snowflake  |
| Variable | `SNOWFLAKE_WAREHOUSE` | Snowflake  |
| Variable | `SNOWFLAKE_DATABASE`  | Snowflake  |
| Variable | `SNOWFLAKE_SCHEMA`    | Snowflake  |
| Variable | `SNOWFLAKE_ROLE`      | Snowflake  |
| Secret   | `DATABRICKS_TOKEN`    | Databricks |
| Variable | `DATABRICKS_HOST`     | Databricks |
| Variable | `DATABRICKS_HTTP_PATH`| Databricks |
| Variable | `DATABRICKS_CATALOG`  | Databricks |
| Variable | `DATABRICKS_SCHEMA`   | Databricks |
| Secret   | `MOTHERDUCK_TOKEN`    | MotherDuck |
| Variable | `MOTHERDUCK_DATABASE` | MotherDuck |

### `sync-warehouse-types.yml`

Run every Monday morning and open a PR automatically if the docs page has
changed (new types added, types removed, or synonyms updated).
