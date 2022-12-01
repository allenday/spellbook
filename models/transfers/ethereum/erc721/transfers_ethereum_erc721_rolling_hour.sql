{{ config(
        alias ='erc721_rolling_hour')
}}

        SELECT
            'ethereum' AS blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() AS updated_at,
            ROW_NUMBER() OVER (PARTITION BY token_address, tokenId ORDER BY hour DESC) AS recency_index
        FROM {{ ref('transfers_ethereum_erc721_agg_hour') }}
