{{ config(
        schema = 'op_retropgf_optimism'
        , alias='round2_voters'
        , materialized='table'
        , tags=['static']
  )
}}

with attestations as (
    select
        *,
        --replace invisible characters
        REGEXP_REPLACE(key, '[[:cntrl:]]', '') as key_mapped
    from {{ ref('optimism_attestationstation_optimism_events') }}

    where
        issuer = '0x60c5c9c98bcbd0b0f2fd89b24c16e533baa8cda3'
        and REGEXP_REPLACE(key, '[[:cntrl:]]', '') = 'retropgf.round-2.can-vote'
        and block_date between cast('2023-02-01' as date) and cast('2023-04-01' as date)
)


select
    v.block_date,
    v.recipient as voter,
    v.issuer,
    v.val_string as can_vote

from (select * from attestations where key_mapped = 'retropgf.round-2.can-vote') as v
