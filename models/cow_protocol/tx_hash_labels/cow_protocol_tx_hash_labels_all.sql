{{ config(
    alias = 'tx_hash_labels_all',
    materialized = 'view')
}}

-- Query Labels
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_stable_to_stable') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_early_investment') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_staking_token_investment') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_harvest_yield') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_onramp') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_offramp') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_bluechip_investment') }}
UNION ALL
SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management') }}