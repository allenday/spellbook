{{config(alias='validators_solana')}}

SELECT DISTINCT
    recipient AS address
    , 'Solana Validator' AS name
    , 'validators' AS category
    , 'soispoke' AS contributor
    , 'query' AS source
    , array('solana') AS blockchain
    , timestamp('2022-10-11') AS created_at
    , now() AS updated_at
FROM {{ source('solana', 'rewards') }}
WHERE reward_type = 'Voting'
