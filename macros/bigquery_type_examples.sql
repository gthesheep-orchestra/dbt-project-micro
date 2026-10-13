{% macro bigquery_type_examples() %}
    {% set types = fromyaml(bigquery_types_yaml()).types %}
    {% for type in types %}
    {{ type.example }} as {{ type.name | lower }}
    {%- if not loop.last %},{% endif %}

    {% endfor %}
{% endmacro %}
