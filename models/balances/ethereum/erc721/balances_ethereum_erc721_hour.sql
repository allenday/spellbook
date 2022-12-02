{{ config(
        alias='erc721_hour',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby", "soispoke", "dot2dotseurat"]\') }}'
        )
}}

WITH
hours AS (
    SELECT explode(
        sequence(
            to_date('2015-01-01'), date_trunc('hour', now()), INTERVAL 1 HOUR
        )
    ) AS hour
)

, daily_balances AS (SELECT
    wallet_address
    , token_address
    , tokenId
    , hour
    , lead(hour, 1, now()) OVER (PARTITION BY token_address, tokenId ORDER BY hour) AS next_hour
    FROM {{ ref('transfers_ethereum_erc721_rolling_hour') }}
)

SELECT DISTINCT
    'ethereum' AS blockchain
    , hours.hour
    , daily_balances.wallet_address
    , daily_balances.token_address
    , daily_balances.tokenId
    , nft_tokens.name AS collection
FROM daily_balances
INNER JOIN hours ON daily_balances.hour <= hours.hour AND hours.hour < daily_balances.next_hour
LEFT JOIN {{ ref('tokens_nft') }} AS nft_tokens ON nft_tokens.contract_address = daily_balances.token_address
    AND nft_tokens.blockchain = 'ethereum'
