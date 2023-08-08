{{
    config(
        alias='likely_bot_labels'
    )
}}

SELECT * FROM {{ ref('labels_optimism_likely_bot_addresses') }}
UNION ALL
SELECT * FROM {{ ref('labels_optimism_likely_bot_contracts') }}