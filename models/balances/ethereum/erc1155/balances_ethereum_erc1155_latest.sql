{{ config(
        alias='erc1155_latest',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
        )
}}
SELECT
    'ethereum' AS blockchain,
    b.wallet_address,
    b.token_address,
    b.tokenid,
    b.amount,
    nft_tokens.name AS collection,
    b.updated_at
FROM {{ ref('transfers_ethereum_erc1155_rolling_day') }} AS b
LEFT JOIN {{ ref('tokens_nft') }} AS nft_tokens
    ON
        nft_tokens.contract_address = b.token_address
        AND nft_tokens.blockchain = 'ethereum'
WHERE
    recency_index = 1
    AND amount > 0
