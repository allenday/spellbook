{{ config(
        alias ='test_dependent_view'
        )
}}

        SELECT
            'ethereum' AS blockchain,
            day,
            wallet_address,
            token_address,
            tokenId,
            current_timestamp() AS updated_at,
            ROW_NUMBER() OVER (PARTITION BY token_address, tokenId, wallet_address ORDER BY day DESC) AS recency_index,
            sum(amount) OVER (
                PARTITION BY token_address, wallet_address ORDER BY day
            ) AS amount
        FROM {{ ref('test_incremental_table') }}