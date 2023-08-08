{{ config(
    alias = 'dao_addresses',
    materialized = 'view')
}}

{% set aragon_models = [
ref('aragon_ethereum_dao_addresses')
,ref('aragon_gnosis_dao_addresses')
,ref('aragon_polygon_dao_addresses')
] %}


SELECT *

FROM (
    {% for dao_model in aragon_models %}
    SELECT
        blockchain,
        dao_creator_tool, 
        dao, 
        dao_wallet_address,
        created_block_time,
        created_date,
        product
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)