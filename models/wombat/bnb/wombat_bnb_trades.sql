{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "wombat_v1",
                                \'["umer_h_adil", "hosuke"]\') }}'
    )
}}

{% set project_start_date = '2022-04-18' %}
{% set wombat_bnb_swap_evt_tables = [
    source('wombat_bnb', 'Pool_evt_Swap')
    , source('wombat_bnb', 'HighCovRatioFeePool_evt_Swap')
    , source('wombat_bnb', 'DynamicPool_evt_Swap')
    , source('wombat_bnb', 'mWOM_Pool_evt_Swap')
    , source('wombat_bnb', 'qWOM_WOMPool_evt_Swap')
    , source('wombat_bnb', 'WMX_WOM_Pool_evt_Swap')
] %}

with wombat_swaps_all_pools as (
    {% for swap_evt_table in wombat_bnb_swap_evt_tables %}
        select
            toamount,
            fromamount,
            totoken,
            fromtoken,
            evt_block_time,
            evt_tx_hash,
            evt_index,
            t.to,
            contract_address
        from {{ swap_evt_table }} as t
        {% if is_incremental() %}
            where evt_block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}

        {% if not loop.last %}
            union all
        {% endif %}

    {% endfor %}
)

select
    "bnb" as blockchain,
    "wombat" as project,
    "1" as version,
    date_trunc("DAY", s.evt_block_time) as block_date,
    s.evt_block_time as block_time,
    CAST(s.toamount as decimal(38, 0)) as token_bought_amount_raw,
    CAST(s.fromamount as decimal(38, 0)) as token_sold_amount_raw,
    coalesce(
        (s.toamount / power(10, prices_b.decimals)) * prices_b.price,
        (s.fromamount / power(10, prices_s.decimals)) * prices_s.price
    ) as amount_usd,
    s.totoken as token_bought_address,
    s.fromtoken as token_sold_address,
    erc20_b.symbol as token_bought_symbol,
    erc20_s.symbol as token_sold_symbol,
    case
        when lower(erc20_b.symbol) > lower(erc20_s.symbol) then concat(erc20_s.symbol, "-", erc20_b.symbol)
        else concat(erc20_b.symbol, "-", erc20_s.symbol)
    end as token_pair,
    s.toamount / power(10, erc20_b.decimals) as token_bought_amount,
    s.fromamount / power(10, erc20_s.decimals) as token_sold_amount,
    coalesce(s.to, tx.from) as taker,
    "" as maker,
    cast(s.contract_address as string) as project_contract_address,
    s.evt_tx_hash as tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    "" as trace_address,
    s.evt_index as evt_index
from
    wombat_swaps_all_pools as s
inner join {{ source('bnb', 'transactions') }} as tx
    on
        tx.hash = s.evt_tx_hash
        {% if not is_incremental() %}
    and tx.block_time >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and tx.block_time >= date_trunc("day", now() - interval "1 week")
        {% endif %}
-- bought tokens
left join {{ ref('tokens_erc20') }} as erc20_b
    on
        erc20_b.contract_address = s.totoken
        and erc20_b.blockchain = "bnb"
-- sold tokens
left join {{ ref('tokens_erc20') }} as erc20_s
    on
        erc20_s.contract_address = s.fromtoken
        and erc20_s.blockchain = "bnb"
-- price of bought tokens
left join {{ source('prices', 'usd') }} as prices_b
    on
        prices_b.minute = date_trunc("minute", s.evt_block_time)
        and prices_b.contract_address = s.totoken
        and prices_b.blockchain = "bnb"
        {% if not is_incremental() %}
    and prices_b.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and prices_b.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
-- price of sold tokens
left join {{ source('prices', 'usd') }} as prices_s
    on
        prices_s.minute = date_trunc("minute", s.evt_block_time)
        and prices_s.contract_address = s.fromtoken
        and prices_s.blockchain = "bnb"
        {% if not is_incremental() %}
    and prices_s.minute >= '{{ project_start_date }}'
    {% endif %}
        {% if is_incremental() %}
            and prices_s.minute >= date_trunc("day", now() - interval "1 week")
        {% endif %}
where
    1 = 1
    {% if is_incremental() %}
        and s.evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
;
