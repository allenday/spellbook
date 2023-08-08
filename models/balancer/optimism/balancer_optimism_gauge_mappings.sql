{{
    config(
        schema = 'balancer_optimism',
        alias='gauge_mappings',
        materialized = 'view',
                        unique_key = ['pool_contract', 'incentives_contract']
    ) 
}}

SELECT distinct 
        'optimism' as blockchain, '2' as version
        , a.pool AS pool_contract, tw.poolId AS pool_id, a.gauge AS incentives_contract, 'rewards gauge' AS incentives_type
        , a.evt_block_time, a.evt_block_number, a.contract_address, a.evt_tx_hash, a.evt_index

FROM (
SELECT pool, gauge, evt_block_time, evt_block_number, contract_address, evt_tx_hash, evt_index FROM {{ source ('balancer_optimism', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= CURRENT_TIMESTAMP() - interval '1 week'
{% endif %}

UNION ALL

SELECT pool, gauge, evt_block_time, evt_block_number, contract_address, evt_tx_hash, evt_index FROM {{ source ('balancer_v2_optimism', 'ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated') }}
{% if is_incremental() %}
WHERE evt_block_time >= CURRENT_TIMESTAMP() - interval '1 week'
{% endif %}
) a
LEFT JOIN {{ source('balancer_v2_optimism', 'Vault_evt_PoolRegistered') }} tw
    ON a.pool = tw.poolAddress