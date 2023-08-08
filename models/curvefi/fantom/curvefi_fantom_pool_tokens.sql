{{ config(
    alias = 'pool_tokens',
    partition_by = {"field": "pool"},
    materialized = 'view',
            unique_key = ['pool', 'token_id', 'token_type']
    )
}}
 
WITH 

base_pools as ( 
        SELECT 
            base_pool as pool
        FROM 
        {{ source('curvefi_fantom', 'StableSwap_Factory_evt_BasePoolAdded') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
), 

base_pools_lp_tokens as ( 
        SELECT 
            LOWER(pool) as pool, 
            LOWER(lp_token) as lp_token 
        FROM UNNEST(ARRAY<STRUCT<pool STRING,lp_token STRING>> [STRUCT('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '0x5B5CFE992AdAC0C9D48E05854B2d91C73a003858'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '0xD02a30d33153877BC20e5721ee53DeDEE0422B2F'),
STRUCT('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40')])
), 

hardcoded_underlying as ( 
        SELECT 
            LOWER(pool) as pool, 
            token_id, 
            LOWER(token_address) as token_address
        FROM UNNEST(ARRAY<STRUCT<pool STRING,token_id STRING,token_address STRING>> [STRUCT('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '0', '0x321162Cd933E2Be498Cd2267a90534A804051b11'),
STRUCT('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '1', '0xDBf31dF14B66535aF65AaC99C32e9eA844e14501'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '0', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '1', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '2', '0x049d68029688eAbF473097a2fC38ef61633A3C7A'),
STRUCT('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '0', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'),
STRUCT('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '1', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75')])
), 

base_pools_underlying_tokens_bought as ( 
        SELECT 
            b.pool, 
            a.token_id, 
            a.token_address, 
            'underlying_token_bought' as token_type, 
            'Base Pool' as pool_type
        FROM 
        hardcoded_underlying a 
        INNER JOIN 
        base_pools b 
            ON a.pool = b.pool 
), 

base_pools_pool_tokens as ( 
        SELECT 
            LOWER(pool) as pool, 
            token_id, 
            LOWER(token_address) as token_address, 
            'pool_token' as token_type, 
            'Base Pool' as pool_type
        FROM UNNEST(ARRAY<STRUCT<pool STRING,token_id STRING,token_address STRING>> [STRUCT('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '0', '0x321162Cd933E2Be498Cd2267a90534A804051b11'),
STRUCT('0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604', '1', '0xDBf31dF14B66535aF65AaC99C32e9eA844e14501'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '0', '0x07E6332dD090D287d3489245038daF987955DCFB'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '1', '0xe578C856933D8e1082740bf7661e379Aa2A30b26'),
STRUCT('0x0fa949783947bf6c1b171db13aeacbb488845b3f', '2', '0x940F41F0ec9ba1A34CF001cc03347ac092F5F6B5'),
STRUCT('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '0', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'),
STRUCT('0x27e611fd27b276acbd5ffd632e5eaebec9761e40', '1', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75')])
), 

base_pools_underlying_tokens_sold as ( 
        SELECT 
            pool, 
            token_id, 
            token_address, 
            'underlying_token_sold' as token_type, 
            pool_type
        FROM 
        base_pools_pool_tokens
), 

plain_pools as ( 
        SELECT 
            pool, 
            CAST(token_id AS STRING) as token_id,
            CAST(token_address AS STRING) as token_address,
            token_type
        FROM 
        (
            SELECT
                output_0 as pool,
                POSEXPLODE(_coins) as (token_id, token_address),
                'pool_token' as token_type
            FROM
            {{ source('curvefi_fantom', 'StableSwap_Factory_call_deploy_plain_pool') }}
            WHERE call_success = true 
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
            {% endif %}
        ) x 
        WHERE x.pool IS NOT NULL 
),

harcoded_plainpools as ( 

        SELECT 
            LOWER(pool) as pool, 
            token_id, 
            LOWER(token_address) as token_address,
            'pool_token' as token_type 
        FROM UNNEST(ARRAY<STRUCT<pool STRING,token_id STRING,token_address STRING>> [STRUCT('0x872686B519E06B216EEf150dC4914f35672b0954', '0', '0x04068da6c83afcfa0e13ba15a6696662335d5b75'),
STRUCT('0x872686B519E06B216EEf150dC4914f35672b0954', '1', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E'),
STRUCT('0x872686B519E06B216EEf150dC4914f35672b0954', '2', '0x9879abdea01a879644185341f7af7d8343556b7a'),
STRUCT('0x872686B519E06B216EEf150dC4914f35672b0954', '3', '0xdc301622e621166bd8e82f2ca0a26c13ad0be355')])
),

plain_pools_pool_tokens as (
        SELECT 
            *, 
            'Plain Pool' as pool_type 
        FROM 
        plain_pools
        WHERE token_address != '0x0000000000000000000000000000000000000000' 

        UNION ALL 

        SELECT 
            *,
            'Plain Pool' as pool_type
        FROM 
        harcoded_plainpools
), 

meta_pools as ( 
        SELECT 
            pool, 
            CAST(token_id AS STRING) as token_id,
            CAST(token_address AS STRING) as token_address,
            base_pool
        FROM 
        (
            SELECT
                output_0 as pool,
                '0' as token_id, 
                _coin as token_address,
                _base_pool as base_pool
            FROM
            {{ source('curvefi_fantom', 'StableSwap_Factory_call_deploy_metapool') }}
            WHERE call_success = true 
            {% if is_incremental() %}
            AND call_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
            {% endif %}

            UNION ALL

            SELECT
                a.output_0 as pool,
                '1' as token_id,
                b.lp_token as token_address, 
                a._base_pool as base_pool
            FROM
            {{ source('curvefi_fantom', 'StableSwap_Factory_call_deploy_metapool') }} a 
            INNER JOIN 
            base_pools_lp_tokens b 
                ON a._base_pool = b.pool
                AND a.call_success = true 
                {% if is_incremental() %}
                AND a.call_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
                {% endif %}
        ) x 
), 

meta_pools_pool_tokens as ( 
        SELECT 
            pool, 
            token_id, 
            token_address, 
            'pool_token' as token_type, 
            'Meta Pool' as pool_type
        FROM 
        meta_pools
), 

meta_pool_distinct as ( 
        SELECT DISTINCT 
            pool, 
            base_pool
        FROM 
        meta_pools
), 

meta_pools_underlying_tokens_bought as ( 
        SELECT 
            pool, 
            token_address, 
            token_id,
            'underlying_token_bought' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '0'
        
        UNION ALL
        
        SELECT 
            mt.pool, 
            bt.token_address, 
            CASE 
                WHEN bt.token_id = '0' THEN '1' 
                WHEN bt.token_id = '1' THEN '2'
                WHEN bt.token_id = '2' THEN '3'
            END as token_id, 
            'underlying_token_bought' as token_type, 
            'Meta Pool' as pool_type
        FROM 
        meta_pool_distinct mt 
        INNER JOIN 
        base_pools_pool_tokens bt 
            ON mt.base_pool = bt.pool
), 

meta_pools_underlying_tokens_sold as ( 
        SELECT 
            pool, 
            token_address, 
            token_id,
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '0'
        
        UNION ALL  

        SELECT 
            pool, 
            token_address, 
            token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '1'
        
        UNION ALL

        SELECT 
            pool, 
            token_address, 
            '2' as  token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '1'
        
        UNION ALL
        
        SELECT 
            pool, 
            token_address, 
            '3' as  token_id, 
            'underlying_token_sold' as token_type,
            'Meta Pool' as pool_type
        FROM 
        meta_pools_pool_tokens
        WHERE token_id = '1'
), 

hardcoded_pools as ( 
    SELECT 
        LOWER(pool) as pool, 
        token_id, 
        LOWER(token_address) as token_address,
        token_type, 
        pool_type 
    FROM UNNEST(ARRAY<STRUCT<pool STRING,token_id STRING,token_address STRING,token_type STRING,pool_type STRING>> [STRUCT('0x3a1659Ddcf2339Be3aeA159cA010979FB49155FF', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'pool_token', 'Plain Pool'),
STRUCT('0x3a1659Ddcf2339Be3aeA159cA010979FB49155FF', '1', '0x321162Cd933E2Be498Cd2267a90534A804051b11', 'pool_token', 'Plain Pool'),
STRUCT('0x3a1659Ddcf2339Be3aeA159cA010979FB49155FF', '2', '0x74b23882a30290451A17c44f4F05243b6b58C76d', 'pool_token', 'Plain Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'pool_token', 'Meta Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '1', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40', 'pool_token', 'Meta Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'underlying_token_sold', 'Meta Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '1', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40', 'underlying_token_sold', 'Plain Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '2', '0x27E611FD27b276ACbd5Ffd632E5eAEBEC9761E40', 'underlying_token_sold', 'Plain Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '0', '0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'underlying_token_bought', 'Meta Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '1', '0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E', 'underlying_token_bought', 'Meta Pool'),
STRUCT('0x92D5ebF3593a92888C25C0AbEF126583d4b5312E', '2', '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75', 'underlying_token_bought', 'Meta Pool')])
),

all_pool_tokens as (
        SELECT pool, token_id, token_address, token_type, pool_type FROM base_pools_pool_tokens
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM base_pools_underlying_tokens_bought
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM base_pools_underlying_tokens_sold
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM plain_pools_pool_tokens
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM meta_pools_pool_tokens
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM meta_pools_underlying_tokens_bought
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM meta_pools_underlying_tokens_sold
        UNION ALL 
        SELECT pool, token_id, token_address, token_type, pool_type FROM hardcoded_pools
)

SELECT 
    'fantom' as blockchain, 
    'curve' as project, 
    '2' as version, 
    pool, 
    token_id, 
    token_address, 
    token_type, 
    pool_type
FROM 
all_pool_tokens