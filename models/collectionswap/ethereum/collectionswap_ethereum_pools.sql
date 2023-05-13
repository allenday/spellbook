{{ config(
        schema='collectionswap_ethereum',
        alias = 'pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_address']
        )
}}

select
    POOLADDRESS as POOL_ADDRESS,
    COLLECTION as NFT_CONTRACT_ADDRESS,
    TOKEN_ADDRESS,
    EVT_TX_HASH as CREATE_TX_HASH,
    EVT_BLOCK_TIME as CREATE_BLOCK_TIME
from {{ source('collectionswap_ethereum','CollectionPoolFactory_evt_NewPool') }} as E
inner join (
    select
        OUTPUT_POOL,
        get_json_object(PARAMS, '$.token') as TOKEN_ADDRESS
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolERC20') }}
    where
        CALL_SUCCESS
        {% if is_incremental() %}
            and CALL_BLOCK_TIME >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    union all
    select
        OUTPUT_POOL,
        '0x0000000000000000000000000000000000000000' as TOKEN_ADDRESS
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolETH') }}
    where
        CALL_SUCCESS
        {% if is_incremental() %}
            and CALL_BLOCK_TIME >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    union all
    select
        OUTPUT_POOL,
        get_json_object(PARAMS, '$.token') as TOKEN_ADDRESS
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolERC20Filtered') }}
    where
        CALL_SUCCESS
        {% if is_incremental() %}
            and CALL_BLOCK_TIME >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    union all
    select
        OUTPUT_POOL,
        '0x0000000000000000000000000000000000000000' as TOKEN_ADDRESS
    from {{ source('collectionswap_ethereum','CollectionPoolFactory_call_createPoolETHFiltered') }}
    where
        CALL_SUCCESS
        {% if is_incremental() %}
            and CALL_BLOCK_TIME >= date_trunc('day', now() - interval '1 week')
        {% endif %}
) as C
    on E.POOLADDRESS = C.OUTPUT_POOL
{% if is_incremental() %}
    where EVT_BLOCK_TIME >= date_trunc('day', now() - interval '1 week')
{% endif %}
