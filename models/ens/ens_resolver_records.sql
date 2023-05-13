{{ config(
    alias = 'resolver_records',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['node','block_time','evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "ens",
                            \'["0xRob"]\') }}'
    )
}}

with resolver_records as (
    select
        a as address,
        node,
        evt_block_time as block_time,
        evt_tx_hash as tx_hash,
        evt_index
    from {{ source('ethereumnameservice_ethereum','PublicResolver_evt_AddrChanged') }}
    {% if is_incremental() %}
        where evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
)

select
    n.name,
    r.address,
    r.node,
    r.block_time,
    r.tx_hash,
    r.evt_index
from resolver_records as r
inner join {{ ref('ens_node_names') }} as n
    on r.node = n.node
