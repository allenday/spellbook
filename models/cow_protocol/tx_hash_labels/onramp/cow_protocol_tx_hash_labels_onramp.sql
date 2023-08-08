{{
    config(
        alias='tx_hash_labels_onramp'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_onramp_ethereum') }}