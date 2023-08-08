{{
    config(
        alias='tx_hash_labels_offramp'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_offramp_ethereum') }}