{{ config(
    schema = 'compound_v2_ethereum',
    alias = 'borrow',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_date', 'evt_block_number', 'evt_index']
    )
}}

with borrows as (
    select
        '2' as version,
        'borrow' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        borrower,
        cast(null as STRING) as repayer,
        cast(null as STRING) as liquidator,
        CAST(borrowAmount AS BIGNUMERIC) / decimals_mantissa as amount,
        CAST(borrowAmount AS BIGNUMERIC) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        TIMESTAMP_TRUNC(evt_block_time, DAY) as block_date
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Borrow') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Borrow') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
    ) evt_borrow
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_borrow.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = TIMESTAMP_TRUNC(evt_borrow.evt_block_time, minute)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
),
repays as (
    select
        '2' as version,
        'repay' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        borrower,
        payer as repayer,
        cast(null as STRING) as liquidator,
        -CAST(repayAmount AS BIGNUMERIC) / decimals_mantissa as amount,
        -CAST(repayAmount AS BIGNUMERIC) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        TIMESTAMP_TRUNC(evt_block_time, DAY) as block_date
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_RepayBorrow') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_RepayBorrow') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
    ) evt_repay
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_repay.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = TIMESTAMP_TRUNC(evt_repay.evt_block_time, minute)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
),
liquidations as (
    select
        '2' as version,
        'borrow_liquidation' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        borrower,
        liquidator as repayer,
        liquidator,
        -CAST(repayAmount AS BIGNUMERIC) / decimals_mantissa as amount,
        -CAST(repayAmount AS BIGNUMERIC) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        TIMESTAMP_TRUNC(evt_block_time, DAY) as block_date
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_LiquidateBorrow') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_LiquidateBorrow') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
    ) evt_liquidate
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_liquidate.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = TIMESTAMP_TRUNC(evt_liquidate.evt_block_time, minute)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
)

select * from borrows
union all
select * from repays
union all
select * from liquidations