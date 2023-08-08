{{
    config(
        alias='tx_hash_labels_bluechip_investment'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_bluechip_investment_ethereum') }}