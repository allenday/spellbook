{{ config(
        alias='erc1155_hour',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
        )
}}

with
hours AS (
    SELECT
        explode(
            sequence(
                to_date('2015-01-01'), date_trunc('hour', now()), INTERVAL 1 hour
            )
        ) AS hour
)

, hourly_balances AS
(SELECT
    wallet_address,
    token_address,
    tokenId,
    hour,
    amount,
    lead(hour, 1, now()) OVER (PARTITION BY token_address, wallet_address ORDER BY hour) AS next_hour
    FROM {{ ref('transfers_ethereum_erc1155_rolling_hour') }})

SELECT
    'ethereum' AS blockchain,
    d.hour,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.amount,
    nft_tokens.name AS collection,
    nft_tokens.category AS category
FROM hourly_balances AS b
INNER JOIN hours AS d ON b.hour <= d.hour AND d.hour < b.next_hour
LEFT JOIN {{ ref('tokens_nft') }} AS nft_tokens ON nft_tokens.contract_address = b.token_address
    AND nft_tokens.blockchain = 'ethereum'
