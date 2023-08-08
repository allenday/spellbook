{{config(alias='usernames')}}


SELECT blockchain, address, name, category, contributor, source, created_at, CURRENT_TIMESTAMP() AS updated_at
FROM {{ref('superrare_ethereum_usernames_seed')}}