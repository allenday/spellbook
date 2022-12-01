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
            row_number() over (partition by token_address, tokenId order by hour desc) AS recency_index
        from {{ ref('transfers_ethereum_erc721_agg_hour') }}
