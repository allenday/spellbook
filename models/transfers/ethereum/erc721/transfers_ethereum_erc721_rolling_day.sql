{{ config(
        alias ='erc721_rolling_day')
}}

        SELECT
            'ethereum' AS blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            cast(current_timestamp AS timestamp) AS updated_at,
            ROW_NUMBER() OVER (PARTITION BY token_address, tokenId ORDER BY day DESC) AS recency_index
        FROM {{ ref('transfers_ethereum_erc721_agg_day') }}
