{% macro snowflake_type_examples() %}
    {% set types = fromyaml(snowflake_types_yaml()).types %}
    {% set storable = types | selectattr('storable') | selectattr('example') | list %}
    {% for type in storable %}
    {{ type.example }} as {{ type.name | lower | replace(' ', '_') }}
    {%- if not loop.last %},{% endif %}

    {% endfor %}
{% endmacro %}
