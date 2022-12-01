{{ config(
        alias ='erc1155_rolling_hour'
        )
}}

        SELECT
            'ethereum' AS blockchain,
            hour,
            wallet_address,
            token_address,
            tokenId,
            cast(current_timestamp AS timestamp) as updated_at,
            row_number() over (partition by token_address, tokenId, wallet_address order by hour desc) AS recency_index,
            sum(amount) over (
                partition by token_address, wallet_address order by hour
            ) AS amount
        from {{ ref('transfers_ethereum_erc1155_agg_hour') }}