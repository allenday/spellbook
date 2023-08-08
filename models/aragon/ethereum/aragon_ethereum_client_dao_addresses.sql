{{ config(
    alias = 'client_dao_addresses',
    partition_by = {"field": "created_date"},
    materialized = 'view',
            unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set project_start_date = '2018-10-27' %}

WITH 

aragon_daos as ( 
        SELECT 
            evt_block_time as created_block_time, 
            TIMESTAMP_TRUNC(evt_block_time, day) as created_date, 
            dao 
        FROM {{ source('aragon_ethereum', 'DAOFactory_evt_DeployDAO') }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
), 

app_ids AS (SELECT * FROM UNNEST(ARRAY<STRUCT<app_id STRING>> [STRUCT(LOWER('0x9ac98dc5f995bf0211ed589ef022719d1487e5cb2bab505676f0d084c07cf89a')),
STRUCT(LOWER('0x701a4fd1f5174d12a0f1d9ad2c88d0ad11ab6aad0ac72b7d9ce621815f8016a9')),
STRUCT(LOWER('0xf2e5eb0f21694bf4e28f98a980dfc4d6a568b5b3e593cfe9cedfd0aed59d8148')),
STRUCT(LOWER('0xbf8491150dafc5dcaee5b861414dca922de09ccffa344964ae167212e8c673ae')),
STRUCT(LOWER('0x5c9918c99c4081ca9459c178381be71d9da40e49e151687da55099c49a4237f1')),
STRUCT(LOWER('0xa9efdd08ab8a16b35803b9887d721f0b9cf17df8ff66b9e57f23bbe4ae5f18ba')),
STRUCT(LOWER('0x7e852e0fcfce6551c13800f1e7476f982525c2b5277ba14b24339c68416336d1'))])), 

get_aragon_wallets as ( 
        SELECT 
            contract_address as dao, 
            CONCAT('0x', SUBSTRING(data, 27, 40)) as dao_wallet_address 
        FROM 
        {{ source('ethereum', 'logs') }}
        {% if not is_incremental() %}
        WHERE block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
        AND topic1 = LOWER('0xd880e726dced8808d727f02dd0e6fdd3a945b24bfee77e13367bcbe61ddbaf47') 
        AND contract_address IN (SELECT dao FROM aragon_daos)
        AND CONCAT('0x', SUBSTRING(data, 131, 64)) IN (SELECT app_id FROM app_ids) 
)

SELECT 
    'ethereum' as blockchain, 
    'aragon' as dao_creator_tool, 
    ad.dao, 
    gw.dao_wallet_address, 
    ad.created_block_time,
    SAFE_CAST(ad.created_date as DATE) as created_date, 
    'aragon_client' as product
FROM 
aragon_daos ad 
LEFT JOIN 
get_aragon_wallets gw  
    ON ad.dao = gw.dao