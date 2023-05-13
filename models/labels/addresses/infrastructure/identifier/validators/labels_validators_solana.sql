{{ config(alias='validators_solana') }}

SELECT DISTINCT
    'solana' AS blockchain,
    recipient AS address,
    'Solana Validator' AS name,
    'infrastructure' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-11') AS created_at,
    now() AS updated_at,
    'validators_solana' AS model_name,
    'identifier' AS label_type
FROM {{ source('solana','rewards') }}
WHERE reward_type = 'Voting'
