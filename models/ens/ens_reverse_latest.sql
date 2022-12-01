{{ config(
    alias = 'reverse_latest',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

--latest Node <> Name relations
with node_names AS (
    SELECT
    name,node,block_time,tx_hash
    FROM (
        SELECT
        case WHEN _name = '0x0000000000000000000000000000000000000000' THEN NULL ELSE _name END AS name
        ,node
        ,call_block_time AS block_time
        ,call_tx_hash AS tx_hash
        ,row_number() over (partition BY node order BY call_block_time DESC) AS ordering --in theory we should also order BY tx_index here
        FROM {{ source('ethereumnameservice_ethereum', 'DefaultReverseResolver_call_setName') }}
        where call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ) foo
    where ordering = 1
)

--static Node <> Address relations
, address_nodes AS (SELECT distinct
    tr.from AS address,
    output AS node
    FROM {{ source('ethereum', 'traces') }} tr
    where success
        AND (to = '0x9062c0a6dbd6108336bcbe4593a3d1ce05512069' -- ReverseRegistrar v1
            or  to = '0x084b1c3c81545d370f3634392de611caabff8148' -- ReverseRegistrar v2
        )
        AND substring(input,1,10) in (
            '0xc47f0027' -- setName(STRING)
            ,'0x0f5a5466' -- claimWithResolver(address,address)
            ,'0x1e83409a' -- claim(address)
            )
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

)

SELECT
    address
    ,name
    ,block_time AS latest_tx_block_time
    ,tx_hash AS latest_tx_hash
    ,an.node AS address_node
FROM address_nodes an
LEFT JOIN node_names nn
ON an.node = nn.node
