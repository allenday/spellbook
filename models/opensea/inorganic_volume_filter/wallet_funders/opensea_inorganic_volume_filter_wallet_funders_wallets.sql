{{ config(
    alias = 'inorganic_volume_filter_wallet_funders_wallet',
    materialized = 'view'
)
}}

SELECT DISTINCT(buyer) AS wallet FROM {{ ref('looksrare_ethereum_trades') }}

UNION

SELECT DISTINCT(seller) AS wallet FROM {{ ref('looksrare_ethereum_trades') }}

UNION

SELECT DISTINCT(buyer) AS wallet FROM {{ ref('x2y2_ethereum_trades') }}

UNION

SELECT DISTINCT(seller) AS wallet FROM {{ ref('x2y2_ethereum_trades') }}

UNION

SELECT DISTINCT(buyer) AS wallet FROM {{ ref('blur_ethereum_trades') }}

UNION

SELECT DISTINCT(seller) AS wallet FROM {{ ref('blur_ethereum_trades') }}