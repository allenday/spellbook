{{
    config(
        alias='tx_hash_labels_early_investment'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_early_investment_ethereum') }}