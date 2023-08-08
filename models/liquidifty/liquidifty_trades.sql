{{ config(
    schema = 'liquidifty',
    alias = 'trades'
)}}

select * from {{ ref('liquidifty_bnb_trades') }}
union all
select * from {{ ref('liquidifty_ethereum_trades') }}