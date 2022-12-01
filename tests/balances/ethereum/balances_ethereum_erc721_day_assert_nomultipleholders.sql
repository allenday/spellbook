-- Check for multiple holders

SELECT
    blockchain,
    day,
    token_address,
    tokenid,
    count(*)
FROM {{ ref('balances_ethereum_erc721_day') }}
WHERE day >= now() - INTERVAL '2 days'
GROUP BY blockchain, day, token_address, tokenid
HAVING count(*) > 1
