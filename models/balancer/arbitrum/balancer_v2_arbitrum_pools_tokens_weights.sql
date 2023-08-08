{{
    config(
        schema='balancer_v2_arbitrum',
        alias='pools_tokens_weights',
        materialized = 'view',
                        unique_key = ['pool_id', 'token_address']
    )
}}

--
-- Balancer v2 Pools Tokens Weights
--
SELECT
    registered.poolId AS pool_id,
    token_address,
    normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPoolFactory_call_create') }} call_create
    ON lower(call_create.output_0) = lower(CAST(SUBSTRING(registered.poolId, 0, 42) as STRING))
    , UNNEST(call_create.tokens) AS token_address WITH OFFSET AS tokens
, UNNEST(call_create.weights) AS normalized_weight WITH OFFSET AS weights
WHERE tokens = weights
    {% if is_incremental() %}
    AND registered.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
UNION ALL

SELECT
    registered.poolId AS pool_id,
    token_address,
    normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPool2TokensFactory_call_create') }} call_create
    ON lower(call_create.output_0) = lower(CAST(SUBSTRING(registered.poolId, 0, 42) as STRING))
    , UNNEST(call_create.tokens) AS token_address WITH OFFSET AS tokens
, UNNEST(call_create.weights) AS normalized_weight WITH OFFSET AS weights
WHERE tokens = weights
    {% if is_incremental() %}
    AND registered.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
UNION ALL

SELECT
    registered.poolId AS pool_id,
    token_address,
    normalized_weight / POWER(10, 18) AS normalized_weight
FROM {{ source('balancer_v2_arbitrum', 'Vault_evt_PoolRegistered') }} registered
INNER JOIN {{ source('balancer_v2_arbitrum', 'WeightedPoolV2Factory_call_create') }} call_create
    ON lower(call_create.output_0) = lower(CAST(SUBSTRING(registered.poolId, 0, 42) as STRING))
    , UNNEST(call_create.tokens) AS token_address WITH OFFSET AS tokens
, UNNEST(call_create.normalizedWeights) AS normalized_weight WITH OFFSET AS weights
WHERE tokens = weights
    {% if is_incremental() %}
    AND registered.evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}