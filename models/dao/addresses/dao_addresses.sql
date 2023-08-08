{{ config(
    alias = 'addresses',
    materialized = 'view')
}}


SELECT * FROM {{ ref('dao_addresses_ethereum') }}

UNION ALL 

SELECT * FROM {{ ref('dao_addresses_gnosis') }}

UNION ALL 

SELECT * FROM {{ ref('dao_addresses_polygon') }}