{{
  config(
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "sudoswap",
                                    \'["ilemi"]\') }}')
}}

SELECT
    blockchain
    , project
    , version
    , block_time
    , collection
    , amount_usd
    , token_standard
    , trade_type
    , trade_category
    , evt_type
    , seller
    , buyer
    , amount_original
    , currency_symbol
    , currency_contract
    , nft_contract_address
    , project_contract_address
    , aggregator_name
    , aggregator_address
    , tx_hash
    , block_number
    , tx_from
    , tx_to
    , unique_trade_id
    , CAST(token_id AS VARCHAR(100)) AS token_id
    , CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items
    , CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw
FROM {{ ref('sudoswap_ethereum_events') }}
WHERE evt_type = 'Trade'
