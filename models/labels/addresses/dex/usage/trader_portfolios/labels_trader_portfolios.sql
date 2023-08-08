{{
    config(
        alias='trader_portfolios'
    )
}}

SELECT * FROM {{ ref('labels_trader_portfolios_ethereum') }}