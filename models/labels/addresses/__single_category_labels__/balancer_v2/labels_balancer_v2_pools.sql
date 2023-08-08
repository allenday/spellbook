{{config(alias='balancer_v2_pools')}}

SELECT * FROM  {{ ref('labels_balancer_v2_pools_ethereum') }}
UNION ALL
SELECT * FROM  {{ ref('labels_balancer_v2_pools_arbitrum') }}
UNION ALL
SELECT * FROM  {{ ref('labels_balancer_v2_pools_optimism') }}
UNION ALL
SELECT * FROM  {{ ref('labels_balancer_v2_pools_polygon') }}