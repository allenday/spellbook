{{
    config(
        alias='balances'
    ) 
}}

{% set balancer_contract = "0xba12222222228d8ba445958a75a0704d566bf2c8" %}

WITH pools AS (
    SELECT pool as pools
    FROM {{ source('balancer_v1_ethereum', 'BFactory_evt_LOG_NEW_POOL') }}
),

joins AS (
    SELECT p.pools as pool, TIMESTAMP_TRUNC(e.evt_block_time, day) AS `day`, e.contract_address AS token, SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} e
    INNER JOIN pools p ON e.`to` = p.pools
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT e.`to` as pool, TIMESTAMP_TRUNC(e.evt_block_time, day) AS `day`, e.contract_address AS token, SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} e
    WHERE e.`to` = '{{balancer_contract}}'
    GROUP BY 1, 2, 3
),

exits AS (
    SELECT p.pools as pool, TIMESTAMP_TRUNC(e.evt_block_time, day) AS `day`, e.contract_address AS token, -SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} e
    INNER JOIN pools p ON e.`from` = p.pools   
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT e.`from` as pool, TIMESTAMP_TRUNC(e.evt_block_time, day) AS `day`, e.contract_address AS token, -SUM(value) AS amount
    FROM {{ source('erc20_ethereum', 'evt_transfer') }} e
    WHERE e.`from` = '{{balancer_contract}}'
    GROUP BY 1, 2, 3
),

daily_delta_balance_by_token AS (
    SELECT pool, `day`, token, SUM(COALESCE(amount, 0)) AS amount FROM 
    (SELECT *
    FROM joins j 
    UNION ALL
    SELECT * 
    FROM exits e) foo
    GROUP BY 1, 2, 3
),

cumulative_balance_by_token AS (
    SELECT 
        pool, 
        token, 
        `day`, 
        LEAD(day, 1, CURRENT_TIMESTAMP()) OVER (PARTITION BY pool, token ORDER BY `day`) AS day_of_next_change,
        SUM(amount) OVER (PARTITION BY pool, token ORDER BY `day` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM daily_delta_balance_by_token
),

calendar AS (
    SELECT TIMESTAMP(day) AS `day` FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2020-01-01'), CURRENT_DATE(), interval 1 DAY)) AS `day`
),

running_cumulative_balance_by_token AS (
    SELECT c.day, pool, token, cumulative_amount 
    FROM calendar c
    LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
)

SELECT * FROM running_cumulative_balance_by_token