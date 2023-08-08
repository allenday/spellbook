{{config(alias='tornado_cash'
)}}

WITH tornado_addresses AS (
SELECT
    lower(blockchain) as blockchain,
    tx_hash,
    depositor AS address,
    'Depositor' as name
FROM {{ ref('tornado_cash_deposits') }}
UNION ALL
SELECT
    lower(blockchain) as blockchain,
    tx_hash,
    recipient AS address,
    'Recipient' as name
FROM {{ ref('tornado_cash_withdrawals') }}
)

SELECT
    blockchain,
    address,
    'Tornado Cash ' || STRING_AGG(collect_set(name),' and ') AS name,
    'tornado_cash' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-01') as created_at,
    CURRENT_TIMESTAMP() as updated_at,
    'tornado_cash' AS model_name,
    'persona' AS label_type
FROM tornado_addresses
GROUP BY address, blockchain