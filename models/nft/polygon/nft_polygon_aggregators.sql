 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name

{% if var('declare_values_with_unnest') %}
FROM UNNEST([
STRUCT<contract_address STRING, name STRING>
{% else %}
FROM (VALUES
{% endif %}

      ('0xb3e808e102ac4be070ee3daac70672ffc7c1adca', 'Element') -- Element NFT Marketplace Aggregator

{% if var('declare_values_with_unnest') %}
])
{% else %}
) AS temp_table (contract_address, name)
{% endif %}
