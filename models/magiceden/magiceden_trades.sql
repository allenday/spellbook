{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "magiceden",
                                    \'["soispoke"]\') }}'
        )
}}

SELECT
    blockchain
    , project
    , version
    , block_time
    , token_id
    , amount_usd
    , token_standard
    , evt_type
    , seller
    , buyer
    , amount_original
    , currency_symbol
    , currency_contract
    , project_contract_address
    , tx_hash
    , unique_trade_id
    , CAST(NULL AS VARCHAR(5)) AS collection
    , CAST(NULL AS VARCHAR(5)) AS trade_type
    , CAST(number_of_items AS DECIMAL(38, 0)) AS number_of_items
    , CAST(NULL AS VARCHAR(5)) AS trade_category
    , CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw
    , CAST(NULL AS VARCHAR(5)) AS nft_contract_address
    , CAST(NULL AS VARCHAR(5)) AS aggregator_name
    , CAST(NULL AS VARCHAR(5)) AS aggregator_address
    , CAST(block_number AS BIGINT) AS block_number
    , CAST(NULL AS VARCHAR(5)) AS tx_from
    , CAST(NULL AS VARCHAR(5)) AS tx_to
FROM {{ ref('magiceden_solana_trades') }}
