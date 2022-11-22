{{ config(
        alias ='erc20_agg_hour',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_transfer_id'
        )
}}

select
    'ethereum' as blockchain,
      {% if date_trunc_variant == 1 %}
        date_trunc('hour', tr.evt_block_time)
      {% elif date_trunc_variant == 2 %}
        date_trunc(tr.evt_block_time, hour)
      {% endif %}
      as hour,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    tr.wallet_address || '-' || tr.token_address || '-' || 
      {% if date_trunc_variant == 1 %}
        date_trunc('hour', tr.evt_block_time)
      {% elif date_trunc_variant == 2 %}
        date_trunc('hour', tr.evt_block_time) 
      {% endif %}
      as unique_transfer_id,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from {{ ref('transfers_ethereum_erc20') }} tr
left join {{ ref('tokens_ethereum_erc20') }} t on t.contract_address = tr.token_address
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where tr.evt_block_time >= 
  {% if date_trunc_variant == 1 %}
    date_trunc('hour', CURRENT_TIMESTAMP - interval '1 week')
  {% elif date_trunc_variant == 2 %}
    date_trunc(CURRENT_TIMESTAMP - interval 1 week, hour)
  {% endif %}
{% endif %}
group by 1, 2, 3, 4, 5, 6
