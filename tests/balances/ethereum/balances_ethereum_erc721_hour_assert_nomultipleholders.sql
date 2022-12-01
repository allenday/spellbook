-- Check for multiple holders

SELECT
    blockchain,
    hour,
    token_address,
    tokenid,
    count(*)
FROM {{ ref('balances_ethereum_erc721_hour') }}
WHERE hour >= now() - INTERVAL '2 hours'
GROUP BY blockchain, hour, token_address, tokenid
HAVING count(*) > 1
