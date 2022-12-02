{{config(alias='view_expirations',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["antonio-mendes", "mewwts"]\') }}')}}
SELECT
    label
    , TO_TIMESTAMP(MIN(expires)) AS min_expires
    , MIN(evt_block_time) AS min_evt_block_time
    , TO_TIMESTAMP(MAX(expires)) AS max_expires
    , MAX(evt_block_time) AS max_evt_block_time
    , COUNT(*) AS count
FROM (
    SELECT
        CONV((id), 10, 16) AS label
        , expires
        , evt_block_time
    FROM
        {{ source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRegistered') }}
    UNION
    SELECT
        CONV((id), 10, 16) AS label
        , expires
        , evt_block_time
    FROM
        {{ source('ethereumnameservice_ethereum', 'BaseRegistrarImplementation_evt_NameRenewed') }}
) AS r
GROUP BY label;
