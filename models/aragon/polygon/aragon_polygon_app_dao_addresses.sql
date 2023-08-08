{{ config(
    alias = 'app_dao_addresses',
    partition_by = {"field": "created_date"},
    materialized = 'view',
            unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

-- query on dune explorer https://dune.com/queries/2412802

{% set project_start_date = '2023-02-27' %}


SELECT 
    'polygon' as blockchain, 
    'aragon' as dao_creator_tool, 
    dao, 
    dao as dao_wallet_address, 
    evt_block_time as created_block_time, 
    SAFE_CAST(TIMESTAMP_TRUNC(evt_block_time, day) as DATE) as created_date, 
    'aragon_app' as product 
FROM 
{{ source('aragon_app_polygon', 'DAORegistry_evt_DAORegistered') }}
{% if not is_incremental() %}
WHERE evt_block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
{% endif %}