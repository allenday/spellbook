{{ config(
        schema = 'op_retropgf_optimism'
        , alias='round2_recipients'
        , materialized='table'
        , tags=['static']
  )
}}

{% set op_token = '0x4200000000000000000000000000000000000042' %}

with attestations as (
    select
        *,
        REGEXP_REPLACE(key, '[[:cntrl:]]', '') as key_mapped
    from {{ ref('optimism_attestationstation_optimism_events') }}

    where
        issuer = '0x60c5c9c98bcbd0b0f2fd89b24c16e533baa8cda3'
        and REGEXP_REPLACE(key, '[[:cntrl:]]', '') in ('retropgf.round-2.name', 'retropgf.round-2.award', 'retropgf.round-2.category')
        and block_date between cast('2023-03-30' as date) and cast('2023-05-01' as date)
)


select
    nm.block_date,
    nm.recipient as submitter_address,
    nm.issuer,
    trim(nm.val_string) as recipient_name,
    trim(ca.val_string) as recipient_category,
    cast(regexp_replace(aw.val_string, '[^0-9\\.]+', '') as double) as award_amount,
    '{{ op_token }}' as award_token

from (select * from attestations where key_mapped = 'retropgf.round-2.name') as nm
left join (select * from attestations where key_mapped = 'retropgf.round-2.award') as aw
    on aw.recipient = nm.recipient
left join (select * from attestations where key_mapped = 'retropgf.round-2.category') as ca
    on ca.recipient = nm.recipient
