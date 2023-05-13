{{ config(alias='aggregators_gem') }}
WITH vasa_contracts AS (
    SELECT DISTINCT address AS contract_address
    FROM {{ source('ethereum','creation_traces') }}
    WHERE
        `from` = '0x073ab1c0cad3677cde9bdb0cdeedc2085c029579'
        AND block_time >= CAST('2021-10-12' AS TIMESTAMP)
)


SELECT
    c.contract_address,
    'Gem' AS name
FROM vasa_contracts AS c
LEFT JOIN {{ source('ethereum','transactions') }} AS t
    ON t.block_time >= CAST('2021-10-12' AS TIMESTAMP) AND t.to = c.contract_address
LEFT JOIN {{ ref('nft_ethereum_transfers') }} AS nt
    ON t.block_number = nt.block_number AND t.hash = nt.tx_hash
GROUP BY 1, 2
HAVING
    count(DISTINCT t.hash) FILTER (WHERE t.`from` != '0x073ab1c0cad3677cde9bdb0cdeedc2085c029579') > 10
    AND count(DISTINCT nt.contract_address) > 2
