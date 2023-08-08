{{ config(
    alias = 'events',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_date', 'unique_trade_id']
    )
}}
{% set eth_address = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' %}
{% set project_start_date = '2021-12-23' %}     -- select min(evt_block_time) from tofu_nft_optimism.MarketNG_evt_EvInventoryUpdate
{% set tofu_fee_address_address = "0xd3cca77cd6dc2794f431ae435323dbe6f9bd82c3" %}


with tff_raw as (
    select 
        call_block_time
        ,call_tx_hash
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(detail, '$.settlement'), '$.feeRate') / 1000000     as fee_rate
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(detail, '$.settlement'), '$.royaltyRate') / 1000000 as royalty_rate
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(detail, '$.settlement'), '$.feeAddress')            as fee_address
        ,JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(detail, '$.settlement'), '$.royaltyAddress')        as royalty_address
        ,posexplode(from_json(JSON_EXTRACT_SCALAR(detail, '$.bundle'), 'array<string>'))         as (i,t)
        ,json_array_length(JSON_EXTRACT_SCALAR(detail, '$.bundle'))                              as bundle_size
    from {{ source('tofu_nft_optimism', 'MarketNG_call_run') }}
    where 
        call_success = true 
        {% if is_incremental() %}
        and call_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
)
,tff as (
    select 
        call_block_time
        ,call_tx_hash
        ,fee_rate
        ,royalty_rate
        ,fee_address
        ,case 
          when royalty_address = '{{tofu_fee_address_address}}' and (royalty_rate is null or royalty_rate = 0)
          then null 
          else royalty_address
        end as royalty_address
        ,bundle_size
        ,JSON_EXTRACT_SCALAR(t, '$.token')   as token
        ,JSON_EXTRACT_SCALAR(t, '$.tokenId') as token_id
        ,JSON_EXTRACT_SCALAR(t, '$.amount')  as amount
        ,i as bundle_index
    from tff_raw
)
,tfe as (
    select
        evt_tx_hash
        ,evt_block_time
        ,evt_block_number
        ,evt_index
        ,JSON_EXTRACT_SCALAR(inventory, '$.seller')   as seller
        ,JSON_EXTRACT_SCALAR(inventory, '$.buyer')    as buyer
        ,JSON_EXTRACT_SCALAR(inventory, '$.kind')     as kind
        ,JSON_EXTRACT_SCALAR(inventory, '$.price')    as price
        ,case when JSON_EXTRACT_SCALAR(inventory, '$.currency') = '0x0000000000000000000000000000000000000000'
            then '{{eth_address}}'
            else JSON_EXTRACT_SCALAR(inventory, '$.currency')
        end as currency
        ,(JSON_EXTRACT_SCALAR(inventory, '$.currency') = '0x0000000000000000000000000000000000000000') as native_eth
        ,contract_address
    from {{ source('tofu_nft_optimism', 'MarketNG_evt_EvInventoryUpdate') }}
    where 
        JSON_EXTRACT_SCALAR(inventory, '$.status') = '1'
        {% if is_incremental() %}
        and evt_block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
        {% endif %}
)
select
    'optimism' as blockchain
    ,'tofu' as project 
    ,'v1' as version
    ,TIMESTAMP_TRUNC(tfe.evt_block_time, day) as block_date
    ,tfe.evt_block_time as block_time
    ,tfe.evt_block_number as block_number 
    ,tff.token_id
    ,nft.standard as token_standard
    ,nft.name as collection 
    ,case when tff.bundle_size = 1 then 'Single Item Trade' else 'Bundle Trade' end as trade_type 
    ,cast(tff.amount as BIGNUMERIC) as number_of_items
    ,'Trade' as evt_type
    ,tfe.seller 
    ,tfe.buyer
    ,case
        when tfe.kind = '1' then 'Buy'
        when tfe.kind = '2' then 'Sell'
        else 'Auction'
    end as trade_category
    ,cast(tfe.price as BIGNUMERIC) as amount_raw 
    ,tfe.price / power(10, pu.decimals) as amount_original
    ,pu.price * tfe.price / power(10, pu.decimals) as amount_usd
    ,case when tfe.native_eth then 'ETH' else pu.symbol end as currency_symbol
    ,tfe.currency as currency_contract
    ,tfe.contract_address as project_contract_address
    ,tff.token as nft_contract_address
    ,agg.name as aggregator_name
    ,agg.contract_address as aggregator_address
    ,tfe.evt_tx_hash as tx_hash
    ,tx.from as tx_from
    ,tx.to as tx_to
    ,cast(tfe.price * tff.fee_rate as FLOAT64) as platform_fee_amount_raw
    ,cast(tfe.price * tff.fee_rate / power(10, pu.decimals) as FLOAT64) as platform_fee_amount
    ,cast(pu.price * tfe.price * tff.fee_rate / power(10, pu.decimals) as FLOAT64) as platform_fee_amount_usd
    ,cast(100 * tff.fee_rate as FLOAT64) as platform_fee_percentage
    ,tfe.price * tff.royalty_rate as royalty_fee_amount_raw
    ,tfe.price * tff.royalty_rate / power(10, pu.decimals) as royalty_fee_amount
    ,pu.price * tfe.price * tff.royalty_rate / power(10, pu.decimals) as royalty_fee_amount_usd
    ,cast(100 * tff.royalty_rate as FLOAT64) as royalty_fee_percentage
    ,tff.royalty_address as royalty_fee_receive_address
    ,case when tfe.native_eth then 'ETH' else pu.symbol end as royalty_fee_currency_symbol
    ,concat(tfe.evt_block_number, tfe.evt_tx_hash, tfe.evt_index, tff.bundle_index) as unique_trade_id
from tfe 
join tff 
    on tfe.evt_tx_hash = tff.call_tx_hash
    and tfe.evt_block_time = tff.call_block_time
inner join {{ source('optimism', 'transactions') }} as tx 
    on tx.block_time = tfe.evt_block_time
    and tx.hash = tfe.evt_tx_hash
    {% if not is_incremental() %}
    and tx.block_time >= '{{project_start_date}}'
    {% else %}
    and tx.block_time >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_nft') }} as nft 
    on nft.contract_address = tff.token
    and nft.blockchain = 'optimism'
left join {{ source('prices', 'usd') }} as pu 
    on pu.blockchain = 'optimism'
    and pu.minute = TIMESTAMP_TRUNC(tfe.evt_block_time, minute)
    and pu.contract_address = tfe.currency
    {% if not is_incremental() %}
    and pu.minute >= '{{project_start_date}}'
    {% else %}
    and pu.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}
left join {{ ref('nft_aggregators') }} as agg
    on agg.contract_address = tx.to 
    and agg.blockchain = 'optimism'