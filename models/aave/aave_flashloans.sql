{{ config(
      alias='flashloans'
      , materialized = 'view'
      , file_format = 'delta'
      , incremental_strategy = 'merge'
      , unique_key = ['blockchain', 'tx_hash', 'evt_index']
      
  )
}}

{% set aave_models = [
ref('aave_arbitrum_flashloans')
, ref('aave_avalanche_c_flashloans')
, ref('aave_optimism_flashloans')
, ref('aave_ethereum_flashloans')
, ref('aave_polygon_flashloans')
, ref('aave_fantom_flashloans')
] %}

SELECT *
FROM (
    {% for aave_model in aave_models %}
      SELECT blockchain
      , project
      , version
      , block_time
      , block_number
      , amount
      , amount_usd
      , tx_hash
      , evt_index
      , fee
      , currency_contract
      , currency_symbol
      , recipient
      , contract_address
    FROM {{ aave_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %} 
)