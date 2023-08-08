{{config(alias='smart_dex_traders')}}

SELECT * FROM {{ ref('labels_smart_dex_traders_ethereum') }}