{% macro optimize_spell(this, materialization) %}
{%- if target.name == 'prod' and materialization in ('table', 'incremental') and target.type == 'databricks' -%}
        OPTIMIZE {{this}};
{%- else -%}
{%- endif -%}
{%- endmacro -%}
