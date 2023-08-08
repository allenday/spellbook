{{
    config(
        alias='tx_hash_labels_harvest_yield'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_harvest_yield_ethereum') }}