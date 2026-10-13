{% macro databricks_type_examples() %}
    {% set types = fromyaml(databricks_types_yaml()).types %}
    {% set usable = types
        | selectattr('storable', 'ne', false)
        | selectattr('example')
        | list %}
    {% for type in usable %}
    {{ type.example }} as {{ type.name | lower | replace(' ', '_') }}
    {%- if not loop.last %},{% endif %}

    {% endfor %}
{% endmacro %}
