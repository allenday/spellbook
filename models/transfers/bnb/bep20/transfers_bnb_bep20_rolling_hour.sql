{{ config(
        alias ='bep20_rolling_hour'
        )
}}

SELECT
    'bnb' AS blockchain,
    hour,
    wallet_address,
    token_address,
    symbol,
    current_timestamp() AS last_updated,
    row_number() over (partition by token_address, wallet_address order by hour desc) AS recency_index,
    sum(amount_raw) over (
        partition by token_address, wallet_address order by hour
    ) AS amount_raw,
    sum(amount) over (
        partition by token_address, wallet_address order by hour
    ) AS amount
from {{ ref('transfers_bnb_bep20_agg_hour') }}
;