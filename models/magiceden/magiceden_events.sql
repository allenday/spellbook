{{ config(
        alias ='events',
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
    , block_date
    , block_time
    , token_id
    , amount_usd
    , token_standard
    , trade_type
    , number_of_items
    , evt_type
    , seller
    , buyer
    , amount_original
    , amount_raw
    , currency_symbol
    , currency_contract
    , project_contract_address
    , tx_hash
    , block_number
    , platform_fee_amount_raw
    , platform_fee_amount
    , platform_fee_amount_usd
    , platform_fee_percentage
    , royalty_fee_amount_raw
    , royalty_fee_amount
    , royalty_fee_amount_usd
    , royalty_fee_percentage
    , royalty_fee_currency_symbol
    , unique_trade_id
    , CAST(NULL AS VARCHAR(5)) AS collection
    , CAST(NULL AS VARCHAR(5)) AS trade_category
    , CAST(NULL AS VARCHAR(5)) AS nft_contract_address
    , CAST(NULL AS VARCHAR(5)) AS aggregator_name
    , CAST(NULL AS VARCHAR(5)) AS aggregator_address
    , CAST(NULL AS VARCHAR(5)) AS tx_from
    , CAST(NULL AS VARCHAR(5)) AS tx_to
    , CAST(NULL AS VARCHAR(5)) AS royalty_fee_receive_address
FROM {{ ref('magiceden_solana_events') }}
