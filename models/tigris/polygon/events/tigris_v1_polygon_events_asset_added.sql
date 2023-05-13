{{ config(
    schema = 'tigris_v1_polygon',
    alias = 'events_asset_added'
    )
 }}

SELECT
    evt_tx_hash,
    _asset AS asset_id,
    _name AS pair
FROM
    {{ source('tigristrade_polygon', 'PairsContract_evt_AssetAdded') }};
