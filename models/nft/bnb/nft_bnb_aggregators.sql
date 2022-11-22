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

      ('0x56085ea9c43dea3c994c304c53b9915bff132d20', 'Element') -- Element NFT Marketplace Aggregator

{% if var('declare_values_with_unnest') %}
])
{% else %}
) AS temp_table (contract_address, name)
{% endif %}
