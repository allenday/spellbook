{{ config(
    schema = 'aztec_v2_ethereum',
    alias = 'deposit_assets',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aztec_v2",
                                \'["Henrystats"]\') }}')
}}

WITH

assets_added AS (
        SELECT
            0 AS asset_id,
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' AS asset_address,
            NULL AS asset_gas_limit,
            NULL AS date_added

        UNION

        SELECT
            assetId AS asset_id,
            assetAddress AS asset_address,
            assetGasLimit AS asset_gas_limit,
            evt_block_time AS date_added
        FROM
        {{source('aztec_v2_ethereum', 'RollupProcessor_evt_AssetAdded')}}
)

SELECT
    a.*,
    t.symbol,
    t.decimals
FROM
assets_added a
LEFT JOIN
{{ ref('tokens_erc20') }} t
    ON a.asset_address = t.contract_address
    AND t.blockchain = 'ethereum'
