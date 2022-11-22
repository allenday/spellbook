{{config(alias='cex')}}

{% if var('declare_values_with_unnest') %}
{% set array_start = '[' %}
{% set array_end = ']' %}
{% else %}
{% set array_start = 'ARRAY(' %}
{% set array_end = ')' %}
{% endif %}

SELECT * FROM {{ ref('labels_cex_ethereum') }}

UNION All

-- add address list from CEXs
SELECT 
{{ array_start }}"optimism"{{ array_end }}, address, distinct_name, 'cex', 'msilb7','static',CAST('2022-10-10' AS TIMESTAMP),CURRENT_TIMESTAMP
FROM {{ ref('addresses_optimism_cex') }}
