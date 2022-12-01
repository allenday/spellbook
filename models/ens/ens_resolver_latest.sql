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
FROM(
     SELECT
     *
    ,row_number() over (partition BY node order BY block_time desc, evt_index desc) AS ordering
    FROM {{ ref('ens_resolver_records')}}
) f
where ordering = 1
