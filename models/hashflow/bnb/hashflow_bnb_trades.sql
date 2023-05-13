{{ config(
    alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                      "project",
                                      "hashflow",
                                    \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2022-01-29' %}
{% set hashflow_bnb_evt_trade_tables = [
    source('hashflow_bnb', 'Pool_evt_Trade')
    , source('hashflow_bnb', 'Pool_evt_LzTrade')
    , source('hashflow_bnb', 'Pool_evt_XChainTrade')
] %}

with dexs as (
    {% for evt_trade_table in hashflow_bnb_evt_trade_tables %}
        select
            evt_block_time as block_time,
            trader as taker,
            '' as maker,
            quotetokenamount as token_bought_amount_raw,
            basetokenamount as token_sold_amount_raw,
            cast(NULL as double) as amount_usd,
            quotetoken as token_bought_address,
            basetoken as token_sold_address,
            contract_address as project_contract_address,
            evt_tx_hash as tx_hash,
            '' as trace_address,
            evt_index
        from
            {{ evt_trade_table }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
            where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        {% if not loop.last %}
            union all
        {% endif %}

    {% endfor %}
)

select
    'bnb' as blockchain,
    'hashflow' as project,
    '1' as version,
    try_cast(date_trunc('DAY', dexs.block_time) as date) as block_date,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol)
            then concat(erc20b.symbol, '-', erc20a.symbol)
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
inner join {{ source('bnb', 'transactions') }} as tx
    on
        dexs.tx_hash = tx.hash
        {% if not is_incremental() %}
    AND tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and tx.block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
left join {{ ref('tokens_erc20') }} as erc20a
    on
        erc20a.contract_address = dexs.token_bought_address
        and erc20a.blockchain = 'bnb'
left join {{ ref('tokens_erc20') }} as erc20b
    on
        erc20b.contract_address = dexs.token_sold_address
        and erc20b.blockchain = 'bnb'
left join {{ source('prices', 'usd') }} as p_bought
    on
        p_bought.minute = date_trunc('minute', dexs.block_time)
        and p_bought.contract_address = dexs.token_bought_address
        and p_bought.blockchain = 'bnb'
        {% if not is_incremental() %}
    AND p_bought.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_bought.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
left join {{ source('prices', 'usd') }} as p_sold
    on
        p_sold.minute = date_trunc('minute', dexs.block_time)
        and p_sold.contract_address = dexs.token_sold_address
        and p_sold.blockchain = 'bnb'
        {% if not is_incremental() %}
    AND p_sold.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and p_sold.minute >= date_trunc('day', now() - interval '1 week')
        {% endif %}
