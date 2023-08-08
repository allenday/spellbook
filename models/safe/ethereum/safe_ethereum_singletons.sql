{{ 
    config(
        materialized = 'view',
        alias='singletons'
    ) 
}}


-- Fetch all known singleton/mastercopy addresses used via factories.
-- Prior to 1.3.0, the factory didn't emit the singleton address with the ProxyCreation event,
select distinct masterCopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxy') }}

UNION ALL 

select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_0_0_call_createProxyWithNonce') }}

UNION ALL

select distinct masterCopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxy') }}

UNION ALL 
select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_0_call_createProxyWithNonce') }}

UNION ALL

select distinct masterCopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxy') }}

UNION ALL 

select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithNonce') }}

UNION ALL

select distinct _mastercopy as address 
from {{ source('gnosis_safe_ethereum', 'ProxyFactoryv1_1_1_call_createProxyWithCallback') }}

UNION ALL

select distinct singleton as address 
from {{ source('gnosis_safe_ethereum', 'GnosisSafeProxyFactory_v1_3_0_evt_ProxyCreation') }}