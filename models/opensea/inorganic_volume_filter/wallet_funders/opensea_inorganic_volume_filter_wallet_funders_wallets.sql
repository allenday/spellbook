{{ config(
    alias = 'inorganic_volume_filter_wallet_funders_wallet',
    materialized = 'view'
)
}}

SELECT DISTINCT(buyer) as wallet FROM {{ ref('looksrare_ethereum_trades') }}

UNION ALL 

SELECT DISTINCT(seller) as wallet FROM {{ ref('looksrare_ethereum_trades') }}

UNION ALL 

SELECT DISTINCT(buyer) as wallet FROM {{ ref('x2y2_ethereum_trades') }}

UNION ALL

SELECT DISTINCT(seller) as wallet FROM {{ ref('x2y2_ethereum_trades') }}

UNION ALL 

SELECT DISTINCT(buyer) as wallet FROM {{ ref('blur_ethereum_trades') }}

UNION ALL

SELECT DISTINCT(seller) as wallet FROM {{ ref('blur_ethereum_trades') }}