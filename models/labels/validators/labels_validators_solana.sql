{{config(alias='validators_solana')}}

SELECT distinct
    array('solana') AS blockchain,
    recipient AS address,
    'Solana Validator' AS name,
    'validators' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-11') AS created_at,
    now() AS updated_at
FROM {{ source('solana', 'rewards') }}
where reward_type = "Voting"