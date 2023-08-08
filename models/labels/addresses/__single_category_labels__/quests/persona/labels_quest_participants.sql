{{
    config(
        alias='quest_participants'
    )
}}

SELECT * FROM {{ ref('labels_optimism_coinbase_wallet_quest_participants') }}
UNION ALL
SELECT * FROM {{ ref('labels_optimism_optimism_quest_participants') }}