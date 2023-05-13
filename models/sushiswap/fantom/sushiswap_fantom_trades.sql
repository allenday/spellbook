{{ config( 
    alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-02-26' %} -- min(evt_block_time) from sushi_fantom.Pair_evt_Swap

with dexs as (
    -- Sushiswap
    select
        t.evt_block_time as block_time,
        t.to as taker,
        '' as maker,
        case when amount0out = 0 then amount1out else amount0out end as token_bought_amount_raw,
        case when amount0in = 0 then amount1in else amount0in end as token_sold_amount_raw,
        cast(null as double) as amount_usd,
        case when amount0out = 0 then f.token1 else f.token0 end as token_bought_address,
        case when amount0in = 0 then f.token1 else f.token0 end as token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        '' as trace_address,
        t.evt_index
    from
        {{ source('sushi_fantom', 'Pair_evt_Swap') }} as t
    inner join {{ source('sushi_fantom', 'SushiV2Factory_evt_PairCreated') }} as f
        on f.pair = t.contract_address
    {% if is_incremental() %}
        where t.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
)

select
    'fantom' as blockchain,
    'sushiswap' as project,
    '1' as version,
    try_cast(date_trunc('DAY', dexs.block_time) as date) as block_date,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount,
    CAST(dexs.token_bought_amount_raw as decimal(38, 0)) as token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw as decimal(38, 0)) as token_sold_amount_raw,
    coalesce(
        dexs.amount_usd,
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) as amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx.from) as taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    dexs.trace_address,
    dexs.evt_index
from dexs
inner join {{ source('fantom', 'transactions') }} as tx
    on
        dexs.tx_hash = tx.hash
        {% if not is_incremental() %}
    and tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
left join {{ ref('tokens_erc20') }} as erc20a
    on
        erc20a.contract_address = dexs.token_bought_address
        and erc20a.blockchain = 'fantom'
left join {{ ref('tokens_erc20') }} as erc20b
    on
        erc20b.contract_address = dexs.token_sold_address
        and erc20b.blockchain = 'fantom'
left join {{ source('prices', 'usd') }} as p_bought
    on
        p_bought.minute = date_trunc('minute', dexs.block_time)
        and p_bought.contract_address = dexs.token_bought_address
        and p_bought.blockchain = 'fantom'
        {% if not is_incremental() %}
    and p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
left join {{ source('prices', 'usd') }} as p_sold
    on
        p_sold.minute = date_trunc('minute', dexs.block_time)
        and p_sold.contract_address = dexs.token_sold_address
        and p_sold.blockchain = 'fantom'
        {% if not is_incremental() %}
    and p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
