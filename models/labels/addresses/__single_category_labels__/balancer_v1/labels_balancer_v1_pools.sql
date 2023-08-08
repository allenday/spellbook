{{config(alias='labels_balancer_v1_pools')}}

SELECT * FROM  {{ ref('labels_balancer_v1_pools_ethereum') }}