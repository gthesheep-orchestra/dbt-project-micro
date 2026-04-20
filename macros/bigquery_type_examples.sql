{% macro bigquery_type_examples() %}
    {% set types = fromyaml(load_file('data/bigquery_types.yml')).types %}
    {% for type in types %}
    {{ type.example }} as {{ type.name | lower }}
    {%- if not loop.last %},{% endif %}

    {% endfor %}
{% endmacro %}
