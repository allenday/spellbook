{{
    config(
        alias='tx_hash_labels_stable_to_stable'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_stable_to_stable_ethereum') }}