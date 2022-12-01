{{ config(
    alias = 'resolver_latest',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

SELECT
    name
    ,address
    ,node
    ,block_time
    ,tx_hash
    ,evt_index
from(
     SELECT
     *
    ,row_number() over (partition by node order by block_time desc, evt_index desc) AS ordering
    from {{ ref('ens_resolver_records')}}
) f
where ordering = 1
