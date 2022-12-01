{{config(alias='validators_ethereum')}}

SELECT distinct
    array('ethereum') AS blockchain,
    from AS address,
    'Ethereum Validator' AS name,
    'validators' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-11') AS created_at,
    now() AS updated_at
FROM {{ source('ethereum','traces') }}
WHERE to = lower('0x00000000219ab540356cBB839Cbe05303d7705Fa')
AND success
AND value > 0