{{
    config(
        alias='tx_hash_labels_staking_token_investment'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_staking_token_investment_ethereum') }}