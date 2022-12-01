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
            row_number() over (partition by token_address, tokenId order by day desc) AS recency_index
        FROM {{ ref('transfers_ethereum_erc721_agg_day') }}
