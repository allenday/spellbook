{{ config( 
    alias='nft',
    materialized = 'view')}}


{% set sources = [
    ('ethereum',   ref('tokens_ethereum_nft'))
] %}

SELECT *
FROM (
    {% for source in sources %}
    SELECT
    '{{ source[0] }}' as blockchain,
    contract_address,
    name,
    symbol,
    standard
    FROM {{ source[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)