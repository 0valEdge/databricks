{% macro combine_dicts(dict1, dict2) %}
    {%- set combined_dict = dict1.copy() -%}
    {%- do combined_dict.update(dict2) -%}
    {{ combined_dict }}
{% endmacro %}