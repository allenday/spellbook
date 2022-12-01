{{ config(
    alias = 'raw_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'composite_index', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "hashflow",
                                \'["justabi", "jeff-dude"]\') }}'
    )
}}

{% set project_start_date = '2021-04-28' %}

with ethereum_traces AS (
    SELECT *
    from {{ source('ethereum', 'traces') }}
    where `to` in ('0x455a3b3be6e7c8843f2b03a1ca22a5a5727ef5c4','0x9d4fc735e1a596420d24a266b7b5402fe4ec153c',
                   '0x2405cb057a9baf85daa11ce9832baed839b6871c','0x043389f397ad72619d05946f5f35426a7ace6613',
                   '0xa18607ca4a3804cc3cd5730eafefcc47a7641643', '0x6ad3dac99c9a4a480748c566ce7b3503506e3d71')
        and block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

ethereum_transactions AS (
    SELECT *
    from {{ source('ethereum', 'transactions') }}
    where block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

prices_usd AS (
    SELECT *
    from {{ source('prices', 'usd') }}
    where `minute` >= '{{ project_start_date }}'
        and blockchain = 'ethereum'
    {% if is_incremental() %}
        and `minute` >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

erc20_tokens AS (
    SELECT *
    from {{ ref('tokens_erc20') }}
    where blockchain = 'ethereum'
),

hashflow_pool_evt_trade AS (
    SELECT *
    from {{ source('hashflow_ethereum', 'pool_evt_trade') }}
    where evt_block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
        and evt_block_time >= date_trunc('day', now() - interval '10 days')
    {% endif %}
),

{% if NOT is_incremental() %}
ethereum_logs AS (
    SELECT *
    from {{ source('ethereum', 'logs') }}
    where block_time >= '{{ project_start_date }}'
        and block_number <= 13974528 -- block of last trade of all legacy routers
),

new_router AS (
    SELECT
        cast(coalesce(l.evt_index, -1) AS int) as composite_index,
        cast(get_json_object(quote,'$.flag') AS string) as source,
        t.call_block_time AS block_time,
        t.call_tx_hash AS tx_hash,
        t.call_success AS fill_status,
        'tradeSingleHop' AS method_id,
        t.contract_address AS router_contract,
        ('0x' || substring(get_json_object(quote,'$.pool') from 3)) AS pool,
        tx.from AS trader,
        ('0x' || substring(get_json_object(quote,'$.quoteToken') from 3)) AS maker_token,
        ('0x' || substring(get_json_object(quote,'$.baseToken') from 3)) AS taker_token,
        case when get_json_object(quote,'$.quoteToken') = '0x0000000000000000000000000000000000000000' then 'ETH'
            else mp.symbol end AS maker_symbol,
        case when get_json_object(quote,'$.baseToken') = '0x0000000000000000000000000000000000000000' then 'ETH'
            else tp.symbol end AS taker_symbol,
        case when l.evt_tx_hash is NOT null then l.`quoteTokenAmount` / power(10, mp.decimals)
            else cast(get_json_object(quote,'$.maxQuoteTokenAmount') AS float) / power(10,mp.decimals) end  as maker_token_amount,
        case when l.evt_tx_hash is NOT null then l.`baseTokenAmount` / power(10, tp.decimals)
            else cast(get_json_object(quote,'$.maxBaseTokenAmount') AS float) / power(10,tp.decimals) end  as taker_token_amount,
        case when l.evt_tx_hash is NOT null
            then coalesce(
                        l.`baseTokenAmount` / power(10, tp.decimals) * tp.price,
                        `quoteTokenAmount` / power(10, mp.decimals) * mp.price)
            else coalesce(
                    cast(get_json_object(quote,'$.maxBaseTokenAmount') AS float) / power(10, tp.decimals) * tp.price,
                    cast(get_json_object(quote,'$.maxQuoteTokenAmount') AS float) / power(10, mp.decimals) * mp.price) end as amount_usd
    from {{ source('hashflow_ethereum', 'router_call_tradesinglehop') }} t
    inner join ethereum_transactions tx on tx.hash = t.call_tx_hash
    LEFT JOIN hashflow_pool_evt_trade l on l.txid = ('0x' || substring(get_json_object(quote,'$.txid') from 3))
    LEFT JOIN prices_usd tp on tp.minute = date_trunc('minute', t.call_block_time)
        and tp.contract_address =
            case when get_json_object(quote,'$.baseToken') = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            else ('0x' || substring(get_json_object(quote,'$.baseToken') from 3)) end
    LEFT JOIN prices_usd mp on mp.minute = date_trunc('minute', t.call_block_time)
        and mp.contract_address =
            case when get_json_object(quote,'$.quoteToken') = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            else ('0x' || substring(get_json_object(quote,'$.quoteToken') from 3)) end
),

event_decoding_legacy_router AS (
    SELECT
        tx_hash,
        index AS evt_index,
        substring(`data`, 13, 20) AS trader,
        substring(`data`, 33, 32) AS tx_id,
        substring(`data`, 109, 20) AS maker_token,
        substring(`data`, 77, 20) AS taker_token,
        cast(conv(substring(`data`, 173, 20), 16, 10) AS decimal) as maker_token_amount,
        cast(conv(substring(`data`, 141, 20), 16, 10) AS decimal) as taker_token_amount
    from ethereum_logs
    where topic1 ='0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e' -- trade0()

    union all

    SELECT
        tx_hash,
        index AS evt_index,
        substring(`data`, 45, 20) AS trader,
        substring(`data`, 65, 32) AS tx_id,
        substring(`data`, 141, 20) AS maker_token,
        substring(`data`, 109, 20) AS taker_token,
        cast(conv(substring(`data`, 205, 20), 16, 10) AS decimal) as maker_token_amount,
        cast(conv(substring(`data`, 173, 20), 16, 10) AS decimal) as taker_token_amount
    from ethereum_logs l
    where topic1 ='0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5' -- trade()
),

legacy_router_w_integration AS (
    SELECT
        cast(coalesce(l.evt_index, -1) AS int) as composite_index,
        substring(input, 324, 1) AS source,
        t.block_time,
        t.tx_hash,
        t.error is null AS fill_status,
        substring(t.input, 1, 4) AS method_id,
        t.to AS router_contract,
        substring(t.input, 17, 20) AS pool,
        tx.from AS trader, -- adjusted to use tx sender due to integration, was substring(t.input, 49, 20) as trader,
        maker_token,
        taker_token,
        case when substring(input, 113, 20) = '0x0000000000000000000000000000000000000000' then 'ETH'
            else mp.symbol end AS maker_symbol,
        case when substring(input, 81, 20) = '0x0000000000000000000000000000000000000000' then 'ETH'
            else tp.symbol end AS taker_symbol,
        case when l.tx_hash is NOT null then maker_token_amount / power(10,mp.decimals) end AS maker_token_amount,
        case when l.tx_hash is NOT null then taker_token_amount / power(10,tp.decimals) end AS taker_token_amount,
        case when l.tx_hash is NOT null then
            coalesce(
                taker_token_amount / power(10, tp.decimals) * tp.price,
                maker_token_amount / power(10, mp.decimals) * mp.price) end AS amount_usd
    from ethereum_traces t
    inner join ethereum_transactions tx on tx.hash = t.tx_hash
    LEFT JOIN event_decoding_legacy_router l on l.tx_id = substring(t.input, 325, 32) -- join on tx_id 1:1, no dup
    LEFT JOIN prices_usd tp on tp.minute = date_trunc('minute', t.block_time)
        and tp.contract_address =
            case when substring(input, 81, 20) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else substring(input, 81, 20) end
    LEFT JOIN prices_usd mp on mp.minute = date_trunc('minute', t.block_time)
        and mp.contract_address =
            case when substring(input, 113, 20) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else substring(input, 113, 20) end
    where -- cast(trace_address AS string) = '{}'  --top level call -- removed this because of 1inch integration
        t.to in ('0xa18607ca4a3804cc3cd5730eafefcc47a7641643')
        and substring(input, 1, 4) in ('0xba93c39c') -- swap
        and t.block_number <= 13803909 -- block of last trade of this legacy router

    union all

    SELECT
        cast(coalesce(l.evt_index, -1) AS int) as composite_index,
        substring(input, 484, 1) AS source,
        t.block_time,
        t.tx_hash,
        t.error is null AS fill_status,
        'tradeSingleHop' AS method_id,
        t.to AS router_contract,
        substring(t.input, 49, 20) AS pool, --mm
        tx.from AS trader,
        maker_token,
        taker_token,
        case when substring(input, 209, 20) = '0x0000000000000000000000000000000000000000' then 'ETH'
            else mp.symbol end AS maker_symbol,
        case when substring(input, 177, 20) = '0x0000000000000000000000000000000000000000' then 'ETH'
            else tp.symbol end AS taker_symbol,
        case when l.tx_hash is NOT null then maker_token_amount / power(10,mp.decimals) end AS maker_token_amount,
        case when l.tx_hash is NOT null then taker_token_amount / power(10,tp.decimals) end AS taker_token_amount,
        case when l.tx_hash is NOT null then
            coalesce(
                taker_token_amount / power(10, tp.decimals) * tp.price,
                maker_token_amount / power(10, mp.decimals) * mp.price) end AS amount_usd
    from ethereum_traces t
    inner join ethereum_transactions tx on tx.hash = t.tx_hash
    LEFT JOIN event_decoding_legacy_router l on l.tx_id = substring(t.input, 485, 32)
    LEFT JOIN prices_usd tp on tp.minute = date_trunc('minute', t.block_time)
        and tp.contract_address =
            case when substring(input, 177, 20) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else substring(input, 177, 20) end
    LEFT JOIN prices_usd mp on mp.minute = date_trunc('minute', t.block_time)
        and mp.contract_address =
            case when substring(input, 209, 20) = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else substring(input, 209, 20) end
    where t.to in ('0x6ad3dac99c9a4a480748c566ce7b3503506e3d71')
        and substring(input, 1, 4) in ('0xf0910b2b') -- trade single hop
        AND t.block_number <= 13974528 -- block of last trade of this legacy router
),
{% endif %}

legacy_routers AS (
    SELECT
        t.block_time,
        t.tx_hash,
        error is null AS fill_status,
        substring(input, 1, 4) AS method_id,
        `to` AS router_contract,
        substring(input, 17, 20) AS pool, --mm
        substring(input, 49, 20) AS trader,
        case when substring(input, 1, 4) = '0xc7f6b19d' then substring(input, 81, 20)
            else '0x0000000000000000000000000000000000000000' end AS maker_token,
        case when substring(input, 1, 4) = '0xc7f6b19d' then '0x0000000000000000000000000000000000000000'
            else substring(input, 81, 20) end AS taker_token, --eth
        case when substring(input, 1, 4) = '0xc7f6b19d' then e.symbol
            else 'ETH' end AS maker_symbol,
        case when substring(input, 1, 4) = '0xc7f6b19d' then 'ETH'
            else e.symbol end AS taker_symbol,
        case when substring(input, 1, 4) = '0xc7f6b19d'
                then cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / power(10, e.decimals)
            else cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / 1e18 end as maker_token_amount,
        case when substring(input, 1, 4) = '0xc7f6b19d'
                then cast(conv(substring(input, 113, 20), 16, 10) AS decimal) / 1e18
            else cast(conv(substring(input, 113, 20), 16, 10) AS decimal) / power(10,e.decimals) end as taker_token_amount,
        case when substring(input, 1, 4) = '0xc7f6b19d'
                then cast(conv(substring(input, 113, 20), 16, 10) AS decimal) / 1e18 * price
            else cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / 1e18 * price end as amount_usd
    from ethereum_traces t
    LEFT JOIN prices_usd p on minute = date_trunc('minute', t.block_time)
    LEFT JOIN erc20_tokens e on e.contract_address = substring(input, 81, 20)
    where cast(trace_address AS string) = '{}'  --top level call
        and `to` in ('0x9d4fc735e1a596420d24a266b7b5402fe4ec153c', '0x2405cb057a9baf85daa11ce9832baed839b6871c')
        and substring(input, 1, 4) in ('0x9ec7605b',  -- token to eth
                                       '0xc7f6b19d') -- eth to token
        and p.symbol = 'WETH'

    union all

    SELECT
            t.block_time,
            t.tx_hash,
            error is null AS fill_status,
            substring(input, 1, 4) AS method_id,
            `to` AS router_contract,
            substring(input, 17, 20) AS pool,
            substring(input, 49, 20) AS trader,
            substring(input, 113, 20) AS maker_token,
            substring(input, 81, 20) AS taker_token,
            mp.symbol AS maker_symbol,
            tp.symbol AS taker_symbol,
            cast(conv(substring(input, 177, 20), 16, 10) AS decimal) / power(10, mp.decimals)  as maker_token_amount,
            cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / power(10, tp.decimals)  as taker_token_amount,
            coalesce(
                cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / power(10, tp.decimals) * tp.price,
                cast(conv(substring(input, 177, 20), 16, 10) AS decimal) / power(10, mp.decimals) * mp.price) as amount_usd
    from ethereum_traces t
    LEFT JOIN prices_usd tp on tp.minute = date_trunc('minute', t.block_time) and tp.contract_address = substring(input, 81, 20)
    LEFT JOIN prices_usd mp on mp.minute = date_trunc('minute', t.block_time) and mp.contract_address = substring(input, 113, 20)
    where cast(trace_address AS string) = '{}'
        and `to` in ('0x455a3B3Be6e7C8843f2b03A1cA22A5a5727ef5C4','0x9d4fc735e1a596420d24a266b7b5402fe4ec153c', '0x2405cb057a9baf85daa11ce9832baed839b6871c','0x043389f397ad72619d05946f5f35426a7ace6613')
        and substring(input, 1, 4) in ('0x064f0410','0x4d0246ad') -- token to token

    union all

    SELECT
        t.block_time,
        t.tx_hash,
        error is null AS fill_status,
        substring(input, 1, 4) AS method_id,
        `to` AS router_contract,
        substring(input, 17, 20) AS pool,
        substring(input, 49, 20) AS trader,
        case when substring(input, 1, 4) = '0xe43d9733' then substring(input, 81, 20)
            else '0x0000000000000000000000000000000000000000' end AS maker_token,
        case when substring(input, 1, 4) = '0xe43d9733' then '0x0000000000000000000000000000000000000000'
            else substring(input, 81, 20) end AS taker_token, --eth
        case when substring(input, 1, 4) = '0xe43d9733' then e.symbol
            else 'ETH' end AS maker_symbol,
        case when substring(input, 1, 4) = '0xe43d9733' then 'ETH'
            else e.symbol end AS taker_symbol,
        case when substring(input, 1, 4) = '0xe43d9733'
                then cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / power(10,e.decimals)
            else cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / 1e18 end as maker_token_amount,
        case when substring(input, 1, 4) = '0xe43d9733'
                then cast(conv(substring(input, 113, 20), 16, 10) AS decimal) / 1e18
            else cast(conv(substring(input, 113, 20), 16, 10) AS decimal) / power(10,e.decimals) end as taker_token_amount,
        case when substring(input, 1, 4) = '0xe43d9733'
                then cast(conv(substring(input, 113, 20), 16, 10) AS decimal) / 1e18 * price
            else cast(conv(substring(input, 145, 20), 16, 10) AS decimal) / 1e18 * price end as amount_usd
    from ethereum_traces t
    LEFT JOIN prices_usd p on minute = date_trunc('minute', t.block_time)
    LEFT JOIN erc20_tokens e on e.contract_address = substring(input, 81, 20)
    where cast(trace_address AS string) = '{}'
        and `to` in ('0x455a3B3Be6e7C8843f2b03A1cA22A5a5727ef5C4','0x043389f397ad72619d05946f5f35426a7ace6613')
        and substring(input, 1, 4) in ('0xd0529c02',  -- token to eth
                                       '0xe43d9733') -- eth to token
        and p.symbol = 'WETH'
),

new_pool AS (
    -- subquery for including new pools created on 2022-04-09
    -- same trade event abi, effectively only from table hashflow.pool_evt_trade since 2022-04-09
    SELECT
        l.evt_index AS composite_index,
        null AS source, -- no join on call for this batch, refer to metabase for source info
        tx.block_time AS block_time,
        tx.hash AS tx_hash,
        true AS fill_status, -- without call we are only logging successful fills
        null AS method_id, -- without call we don't have function call info
        tx.to AS router_contract, -- taking top level contract called in tx as router, NOT necessarily HF contract
        l.pool AS pool,
        tx.from AS trader,
        l.`quoteToken` AS maker_token,
        l.`baseToken` AS taker_token,
        case when l.`quoteToken` = '0x0000000000000000000000000000000000000000' then 'ETH'
            else mp.symbol end AS maker_symbol,
        case when l.`baseToken` = '0x0000000000000000000000000000000000000000' then 'ETH'
            else tp.symbol end AS taker_symbol,
        l.`quoteTokenAmount` / power(10, mp.decimals) AS maker_token_amount,
        l.`baseTokenAmount` / power(10, tp.decimals) AS taker_token_amount,
        coalesce(
                l.`baseTokenAmount` / power(10, tp.decimals) * tp.price,
                l.`quoteTokenAmount` / power(10, mp.decimals) * mp.price) AS amount_usd
    from hashflow_pool_evt_trade l
    inner join ethereum_transactions tx on tx.hash = l.evt_tx_hash
    LEFT JOIN prices_usd tp on tp.minute = date_trunc('minute', tx.block_time)
        and tp.contract_address =
            case when l.`baseToken` = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else l.`baseToken` end
    LEFT JOIN prices_usd mp on mp.minute = date_trunc('minute', tx.block_time)
        and mp.contract_address =
            case when l.`quoteToken` = '0x0000000000000000000000000000000000000000'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' else l.`quoteToken` end
    WHERE l.evt_block_time > '2022-04-08' -- necessary filter to only include new trades
),


{% if NOT is_incremental() %}

dedupe_new_router AS ( -- since new_router and new_pool have overlapping trades, we remove them from new_router here
    SELECT new_router.*
    from new_router
    LEFT JOIN new_pool
    on new_router.block_time = new_pool.block_time
        and new_router.composite_index = new_pool.composite_index
        and new_router.tx_hash = new_pool.tx_hash
    where new_pool.tx_hash is null

),

{% endif %}

all_trades AS (
    SELECT
        -1 AS composite_index,
        -- was decoding from trace, no log_index, only single swap exist so works AS PK
        '0x00' AS source,
        -- all from native front end, no integration yet
        *
    from legacy_routers

    union all

    SELECT * from new_pool

    {% if NOT is_incremental() %}

    union all

    SELECT * from legacy_router_w_integration

    union all

    SELECT * from dedupe_new_router

    {% endif %}
)

SELECT
    try_cast(date_trunc('day', block_time) AS date) AS block_date,
    block_time,
    composite_index,
    fill_status,
    maker_symbol,
    maker_token,
    maker_token_amount,
    method_id,
    pool,
    router_contract,
    source,
    taker_symbol,
    taker_token,
    taker_token_amount,
    trader,
    tx_hash,
    amount_usd
from all_trades
where fill_status is true
;