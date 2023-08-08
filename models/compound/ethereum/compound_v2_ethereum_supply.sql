{{ config(
    schema = 'compound_v2_ethereum',
    alias = 'supply',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_date', 'evt_block_number', 'evt_index']
    )
}}

with mints as (
    select
        '2' as version,
        'deposit' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        minter as depositor,
        cast(null as STRING) as withdrawn_to,
        cast(null as STRING) as liquidator,
        CAST(mintAmount AS BIGNUMERIC) / decimals_mantissa as amount,
        CAST(mintAmount AS BIGNUMERIC) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        TIMESTAMP_TRUNC(evt_block_time, DAY) as block_date
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Mint') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Mint') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
    ) evt_mint
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_mint.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = TIMESTAMP_TRUNC(evt_mint.evt_block_time, minute)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
),
redeems as (
    select
        '2' as version,
        'withdraw' as transaction_type,
        asset_symbol as symbol,
        asset_address as token_address,
        redeemer as depositor,
        redeemer as withdrawn_to,
        cast(null as STRING) as liquidator,
        -CAST(redeemAmount AS BIGNUMERIC) / decimals_mantissa as amount,
        -CAST(redeemAmount AS BIGNUMERIC) / decimals_mantissa * price as usd_amount,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        TIMESTAMP_TRUNC(evt_block_time, DAY) as block_date
    from (
        select * from {{ source('compound_v2_ethereum', 'cErc20_evt_Redeem') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
        union all
        select * from {{ source('compound_v2_ethereum', 'cEther_evt_Redeem') }}
        {% if is_incremental() %}
		where evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
		{% endif %}
    ) evt_mint
    left join {{ ref('compound_v2_ethereum_ctokens') }} ctokens
        on evt_mint.contract_address = ctokens.ctoken_address
    left join {{ source('prices', 'usd') }} p
        on p.minute = TIMESTAMP_TRUNC(evt_mint.evt_block_time, minute)
        and p.contract_address = ctokens.asset_address
        and p.blockchain = 'ethereum'
        {% if is_incremental() %}
        and p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
)


select * from mints
union all
select * from redeems