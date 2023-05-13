{{ config(
        alias='erc721_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}
SELECT
    'ethereum' AS blockchain,
    b.wallet_address,
    b.token_address,
    b.tokenid,
    nft_tokens.name AS collection,
    b.updated_at
FROM {{ ref('transfers_ethereum_erc721_rolling_day') }} AS b
LEFT JOIN {{ ref('tokens_nft') }} AS nft_tokens
    ON
        nft_tokens.contract_address = b.token_address
        AND nft_tokens.blockchain = 'ethereum'
LEFT JOIN {{ ref('balances_ethereum_erc721_noncompliant') }} AS nc
    ON b.token_address = nc.token_address
WHERE
    recency_index = 1
    AND amount = 1
    AND nc.token_address IS NULL
