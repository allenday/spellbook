{{ config(alias='app_data'
)}}

-- Find the PoC Query here: https://dune.com/queries/1751965
with
partially_unpacked_app_content as (
    select
        distinct app_hash,
        content.appCode as app_code,
        content.environment,
        content.metadata.orderClass.orderClass as order_class,
        content.metadata.quote.slippageBips as slippageBips,
        content.metadata.referrer.address as referrer_address,
        content.metadata.referrer.referrer as referrer_referrer
    from {{ source('cowswap', 'raw_app_data') }}
),

unpacked_referrer_app_data as (
    select
        app_hash,
        app_code,
        environment,
        order_class,
        slippageBips,
        -- different app data versions put referrer in two possible places.
        lower(coalesce(referrer_address, referrer_referrer)) as referrer
    from partially_unpacked_app_content
),

results as (
    select
        app_hash,
        app_code,
        environment,
        order_class,
        referrer,
        cast(slippageBips as integer) slippage_bips
    from unpacked_referrer_app_data
)

select * from results
