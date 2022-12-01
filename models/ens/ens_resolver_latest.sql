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
    , address
    , node
    , block_time
    , tx_hash
    , evt_index
FROM(
     SELECT
     *
    , ROW_NUMBER() OVER (PARTITION BY node ORDER BY block_time DESC, evt_index DESC) AS ordering
    FROM {{ ref('ens_resolver_records')}}
) f
where ordering = 1
