{{ 
    config(
        materialized = 'view',
        alias='singletons'
    ) 
}}


-- Fetch all known singleton addresses used via the factory.
select distinct singleton as address 
from {{ source('gnosis_safe_optimism', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}