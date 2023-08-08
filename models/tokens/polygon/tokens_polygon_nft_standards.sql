{{ config(
        alias ='nft_standards',
        materialized = 'view',
                        unique_key = ['contract_address']
)
}}

 SELECT
  t.contract_address
, ARRAY_AGG(t.token_standard ORDER BY t.block_time DESC LIMIT 1)[OFFSET(0)] AS standard
FROM {{ ref('nft_polygon_transfers') }} t
    {% if is_incremental() %}
       WHERE t.block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
GROUP BY 1