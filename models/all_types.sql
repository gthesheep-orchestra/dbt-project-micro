{{ config(materialized='table') }}
 
select
    {% if target.name == 'motherduck' %}
        {{ motherduck_type_examples() }}
    {% elif target.type == 'bigquery' %}
        {{ bigquery_type_examples() }}
    {% elif target.type == 'snowflake' %}
        {{ snowflake_type_examples() }}
    {% elif target.type == 'databricks' %}
        {{ databricks_type_examples() }}
    {% else %}
        {{ exceptions.raise_compiler_error(
            "Unsupported target type '" ~ target.type ~ "'. "
            ~ "Add a macro for this warehouse and register it here."
        ) }}
    {% endif %}
 