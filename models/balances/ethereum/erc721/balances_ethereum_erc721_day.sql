{{ config(
        alias='erc721_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby", "soispoke", "dot2dotseurat"]\') }}'
        )
}}

with
days AS (
    SELECT
        explode(
            sequence(
                to_date('2015-01-01'), date_trunc('day', now()), INTERVAL 1 day
            )
        ) AS day
)

, daily_balances AS
(SELECT
    wallet_address,
    token_address,
    tokenId,
    day,
    lead(day, 1, now()) OVER (PARTITION BY token_address, tokenId ORDER BY day) AS next_day
    FROM {{ ref('transfers_ethereum_erc721_rolling_day') }})

SELECT distinct
    'ethereum' AS blockchain,
    d.day,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    nft_tokens.name AS collection
FROM daily_balances AS b
INNER JOIN days AS d ON b.day <= d.day AND d.day < b.next_day
LEFT JOIN {{ ref('tokens_nft') }} AS nft_tokens ON nft_tokens.contract_address = b.token_address
    AND nft_tokens.blockchain = 'ethereum'
