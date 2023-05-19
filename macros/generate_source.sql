{% macro generate_source(database_name, schema_name, source_name) %}

{% set sql %}
    with "columns" as (
        select '- name: ' || lower(column_name) || '\n            data type: '|| lower(DATA_TYPE) ||'\n            description: "stores data related to || lower(column_name) || in snowflake table || lower(table_name) ||"'
            as column_statement,
            table_name,
            column_name
        from {{database_name}}.information_schema.columns
        where table_schema = '{{schema_name}}' 
    ),
    tables as (
	    select table_name,
        '\n      - name: ' || lower(table_name) || '\n        columns:' || string_agg('\n          ' || column_statement || '\n' order by column_name) as table_desc
        from "columns"
        group by table_name
    )

    select string_agg('\n' || table_desc || '\n', order by table_name)
    from tables;

{% endset %}


{%- call statement('generator', fetch_result=True) -%}
{{ sql }}
{%- endcall -%}

{%- set states=load_result('generator') -%}
{%- set states_data=states['data'] -%}
{%- set states_status=states['response'] -%}

{% set sources_yaml=[] %}
{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ source_name | lower) %}
{% do sources_yaml.append('    description: ""' ) %}
{% do sources_yaml.append('    database: ' ~ database_name | lower) %}
{% do sources_yaml.append('    schema: ' ~ schema_name | lower) %}
{% do sources_yaml.append('    loader: fivetran') %}
{% do sources_yaml.append('    loaded_at_field: _FIVETRAN_SYNCED') %}
{% do sources_yaml.append('    meta:') %}
{% do sources_yaml.append('      owner: ""') %}
{% do sources_yaml.append('      tags: [""]') %}
{% do sources_yaml.append('      subscribers: ["@data-team"]') %}

{% do sources_yaml.append('    tables:' ~ states_data[0][0] ) %}

{% if execute %}

{% set joined = sources_yaml | join ('\n') %}
{{ log(joined, info=True) }}
{% do return(joined) %}

{% endif %}

{% endmacro %}