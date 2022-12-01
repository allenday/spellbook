{{ config(
    alias = 'node_names',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

-- because we don't have the keccak namehash function available in v2
-- we do a little sketchy event matching to get the node <> name relationships
-- basically this takes the last AddrChanged event in the same tx preceding a NameRegistred event to link the node AND the name
-- ONLY works for base ENS names (.eth , no subdomains)
with registrations AS (
    SELECT
        label AS label_hash
        ,name AS label_name
        ,evt_block_number AS block_number
        ,evt_block_time AS block_time
        ,evt_tx_hash AS tx_hash
        ,evt_index
    FROM {{ ref('ens_view_registrations') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

,node_info AS (
    SELECT
        a AS address
        ,node
        ,evt_block_number AS block_number
        ,evt_block_time AS block_time
        ,evt_tx_hash AS tx_hash
        ,evt_index
    FROM {{ source('ethereumnameservice_ethereum','PublicResolver_evt_AddrChanged') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

-- here's the sketchy matching
, matching AS (
    SELECT *
    FROM (
        SELECT *
        ,row_number() over (partition BY node order BY block_time desc, evt_index desc) AS ordering2
        FROM (
            SELECT
            r.*
            ,n.address
            ,n.node
            ,row_number() over (partition BY r.tx_hash order BY (r.evt_index - n.evt_index) asc) AS ordering
            FROM registrations r
            inner join node_info n
            ON r.block_number = n.block_number
            AND r.tx_hash = n.tx_hash
            AND r.evt_index > n.evt_index --register event comes after node event
        )
        where ordering = 1
    )
    where ordering2 = 1
)

SELECT
    node
    ,concat(label_name,'.eth') AS name
    ,label_name
    ,label_hash
    ,address AS initial_address
    ,tx_hash
    ,block_number
    ,block_time
    ,evt_index
FROM matching


