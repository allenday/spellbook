{{ config(
    alias = 'prices',
    partition_by = {"field": "day"},
    materialized = 'view',
            unique_key = ['hour', 'blockchain', 'contract_address']
    )
}}

WITH

dex_trades as (
    SELECT 
        d.token_bought_address as contract_address, 
        COALESCE(d.amount_usd/d.token_bought_amount, d.amount_usd/(d.token_bought_amount_raw/POW(10, er.decimals))) as price, 
        d.block_time, 
        d.blockchain
    FROM {{ ref('dex_trades') }} d 
    LEFT JOIN {{ ref('tokens_erc20') }} er 
        ON d.token_bought_address = er.contract_address
        AND d.blockchain = er.blockchain
    WHERE d.amount_usd > 0 
        AND d.token_bought_amount_raw > 0 
        {% if is_incremental() %}
        AND d.block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}

    UNION ALL

    SELECT 
        d.token_sold_address as contract_address, 
        COALESCE(d.amount_usd/d.token_sold_amount, d.amount_usd/(d.token_sold_amount_raw/POW(10, er.decimals))) as price, 
        d.block_time, 
        d.blockchain
    FROM {{ ref('dex_trades') }} d 
    LEFT JOIN {{ ref('tokens_erc20') }} er 
        ON d.token_sold_address = er.contract_address
        AND d.blockchain = er.blockchain
    WHERE d.amount_usd > 0 
        AND d.token_bought_amount_raw > 0 
        {% if is_incremental() %}
        AND d.block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
)

SELECT 
    SAFE_CAST(TIMESTAMP_TRUNC(hour, day) as date) AS `day`, -- for partitioning 
    * 
FROM 
(
    SELECT 
        TIMESTAMP_TRUNC(block_time, hour) AS `hour`, 
        contract_address,
        blockchain,
        APPROX_QUANTILES(price, 100)[OFFSET(50)] AS median_price,
        COUNT(price) as sample_size 
    FROM dex_trades
    GROUP BY 1, 2, 3
    HAVING COUNT(price) >= 5 
) tmp