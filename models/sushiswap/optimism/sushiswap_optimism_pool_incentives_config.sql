{{ config(
    alias = 'pool_incentives_config'
    , tags=['static']
    )
}}


SELECT 'optimism' as blockchain
     , LOWER(TRIM(contract_address)) as contract_address
     , LOWER(TRIM(reward_token)) as reward_token
     , reward_per_second
     , LOWER(TRIM(masterchef_v2_address)) as masterchef_v2_address
FROM UNNEST(ARRAY<STRUCT<contract_address STRING,reward_token STRING,reward_per_second INT64,masterchef_v2_address INT64>> [STRUCT('0x320a04b981c092884a9783cde907578f613ef773','0x4200000000000000000000000000000000000042',0,'0xb25157bf349295a7cd31d1751973f426182070d6')])