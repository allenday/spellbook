{{ config(
        alias='erc721_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby", "soispoke", "dot2dotseurat"]\') }}'
        )
}}

WITH
days AS (
    SELECT explode(
        sequence(
            to_date('2015-01-01'), date_trunc('day', now()), INTERVAL 1 DAY
        )
    ) AS day
)

, daily_balances AS (SELECT
    wallet_address
    , token_address
    , tokenId
    , day
    , lead(
        day, 1, now()
    ) OVER (PARTITION BY token_address, tokenId ORDER BY day) AS next_day
    FROM {{ ref('transfers_ethereum_erc721_rolling_day') }}
)

SELECT DISTINCT
    'ethereum' AS blockchain
    , days.day
    , daily_balances.wallet_address
    , daily_balances.token_address
    , daily_balances.tokenId
    , nft_tokens.name AS collection
FROM daily_balances
INNER JOIN
    days ON
        daily_balances.day <= days.day AND days.day < daily_balances.next_day
LEFT JOIN
    {{ ref('tokens_nft') }} AS nft_tokens ON
        nft_tokens.contract_address = daily_balances.token_address
        AND nft_tokens.blockchain = 'ethereum'
