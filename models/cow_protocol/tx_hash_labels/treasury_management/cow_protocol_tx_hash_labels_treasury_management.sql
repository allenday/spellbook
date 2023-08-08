{{
    config(
        alias='tx_hash_labels_treasury_management'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management_ethereum') }}