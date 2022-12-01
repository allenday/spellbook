{{
  config(
        alias='contract_mapping',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='contract_address',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "contracts",
                                    \'["msilb7", "chuxin"]\') }}'
  )
}}

-- set max number of levels to trace root contract
{% set max_levels = 5 %}
-- set column names to loop through
{% set cols = [
    "contract_project"
    ,"token_symbol"
    ,"contract_name"
    ,"creator_address"
    ,"created_time"
    ,"contract_factory"
    ,"is_self_destruct"
    ,"creation_tx_hash"
] %}

with base_level AS (
  SELECT
    creator_address
    , contract_factory
    , contract_address
    , created_time
    , creation_tx_hash
    , is_self_destruct
  FROM (
    SELECT
      ct.`FROM` AS creator_address
      , NULL::STRING AS contract_factory
      , ct.address AS contract_address
      , ct.block_time AS created_time
      , ct.tx_hash AS creation_tx_hash
      , coalesce(sd.contract_address is NOT NULL, false) AS is_self_destruct
    FROM {{ source('optimism', 'creation_traces') }} AS ct
    LEFT JOIN {{ ref('contracts_optimism_self_destruct_contracts') }} AS sd
      ON ct.address = sd.contract_address
      AND ct.tx_hash = sd.creation_tx_hash
      AND ct.block_time = sd.created_time
      {% if is_incremental() %}
      AND sd.created_time >= date_trunc('day', now() - interval '1 week')
      {% endif %}
    where
      true
    {% if is_incremental() %}
      AND ct.block_time >= date_trunc('day', now() - interval '1 week')

    -- to get existing history of contract mapping
    union all

    SELECT
      creator_address
      , contract_creator_if_factory AS contract_factory
      , contract_address
      , created_time
      , creation_tx_hash
      , is_self_destruct
    FROM {{ this }}
    {% endif %}
  ) AS x
  GROUP BY 1, 2, 3, 4, 5, 6
)
, tokens AS (
  SELECT
    bl.contract_address
    , t.symbol
  FROM base_level AS bl
  join {{ ref('tokens_optimism_erc20') }} AS t
    ON bl.contract_address = t.contract_address
  GROUP BY 1, 2

  union all

  SELECT
    bl.contract_address
    , t.name AS symbol
  FROM base_level AS bl
  join {{ ref('tokens_optimism_nft') }} AS t
    ON bl.contract_address = t.contract_address
  GROUP BY 1, 2
)
-- starting FROM 0
{% for i in range(max_levels) -%}
, level{{i}} AS (
    SELECT
      {{i}} AS level
      , coalesce(u.creator_address, b.creator_address) AS creator_address
      {% if loop.first -%}
      , case
        when u.creator_address is NULL then NULL
        else b.creator_address
      end AS contract_factory
      {% else -%}
      , case
        when u.creator_address is NULL then b.contract_factory
        else b.creator_address
      end AS contract_factory
      {% endif %}
      , b.contract_address
      , b.created_time
      , b.creation_tx_hash
      , b.is_self_destruct
    {% if loop.first -%}
    FROM base_level AS b
    LEFT JOIN base_level AS u
      ON b.creator_address = u.contract_address
    {% else -%}
    FROM level{{i-1}} AS b
    LEFT JOIN base_level AS u
      ON b.creator_address = u.contract_address
    {% endif %}
)
{%- endfor %}

, creator_contracts AS (
  SELECT
    f.creator_address
    , f.contract_factory
    , f.contract_address
    , coalesce(cc.contract_project, ccf.contract_project) AS contract_project
    , f.created_time
    , f.is_self_destruct
    , f.creation_tx_hash
  FROM level{{max_levels - 1}} AS f
  LEFT JOIN {{ ref('contracts_optimism_contract_creator_address_list') }} AS cc
    ON f.creator_address = cc.creator_address
  LEFT JOIN {{ ref('contracts_optimism_contract_creator_address_list') }} AS ccf
    ON f.contract_factory = ccf.creator_address
  where f.contract_address is NOT NULL
 )
, combine AS (
  SELECT
    cc.creator_address
    , cc.contract_factory
    , cc.contract_address
    , coalesce(cc.contract_project, oc.namespace) AS contract_project
    , oc.name AS contract_name
    , cc.created_time
    , coalesce(cc.is_self_destruct, false) AS is_self_destruct
    , 'creator contracts' AS source
    , cc.creation_tx_hash
  FROM creator_contracts AS cc
  LEFT JOIN {{ source('optimism', 'contracts') }} AS oc
    ON cc.contract_address = oc.address

  union all
  -- ovm 1.0 contracts

  SELECT
    creator_address
    , NULL AS contract_factory
    , contract_address
    , contract_project
    , contract_name
    , to_timestamp(created_time) AS created_time
    , false AS is_self_destruct
    , 'ovm1 contracts' AS source
    , NULL AS creation_tx_hash
  FROM {{ source('ovm1_optimism', 'contracts') }} AS c
  where
    true
    {% if is_incremental() %} -- this filter will only be applied ON an incremental run
    AND NOT exists (
      SELECT 1
      FROM {{ this }} AS gc
      where
        gc.contract_address = c.contract_address
        AND (
          (gc.contract_project = c.contract_project) or (gc.contract_project is NULL)
        )
    )
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9

  union all
  --synthetix genesis contracts

  SELECT
    NULL AS creator_address
    , NULL AS contract_factory
    , snx.contract_address
    , 'Synthetix' AS contract_project
    , contract_name
    , to_timestamp('2021-07-06 00:00:00') AS created_time
    , false AS is_self_destruct
    , 'synthetix contracts' AS source
    , NULL AS creation_tx_hash
  FROM {{ source('ovm1_optimism', 'synthetix_genesis_contracts') }} AS snx
  where
    true
    {% if is_incremental() %} -- this filter will only be applied ON an incremental run
    AND NOT exists (
      SELECT 1
      FROM {{ this }} AS gc
      where
        gc.contract_address = snx.contract_address
        AND gc.contract_project = 'Synthetix'
    )
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
, get_contracts AS (
  SELECT
    c.contract_address
    , c.contract_factory
    , c.contract_project
    , t.symbol AS token_symbol
    , c.contract_name
    , c.creator_address
    , c.created_time
    , c.is_self_destruct
    , c.creation_tx_hash
  FROM combine AS c
  LEFT JOIN tokens AS t
    ON c.contract_address = t.contract_address
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
, cleanup AS (
--grab the first non-NULL value for each, i.e. if we have the contract via both contract mapping AND optimism.contracts
  SELECT
    contract_address
    {% for col in cols %}
    , (array_agg({{ col }}) filter (where {{ col }} is NOT NULL))[0] AS {{ col }}
    {% endfor %}
  FROM get_contracts
  where contract_address is NOT NULL
  GROUP BY 1
)
SELECT
  c.contract_address
  , initcap(
      replace(
      -- priority order: Override name, Mapped vs Dune, Raw / Actual names
        coalesce(
          co.contract_project
          , dnm.mapped_name
          , c.contract_project
          , ovm1c.contract_project
        ),
      '_',
      ' '
    )
   ) AS contract_project
  , c.token_symbol
  , coalesce(co.contract_name, c.contract_name) AS contract_name
  , coalesce(c.creator_address, ovm1c.creator_address) AS creator_address
  , coalesce(c.created_time, to_timestamp(ovm1c.created_time)) AS created_time
  , c.contract_factory AS contract_creator_if_factory
  , coalesce(c.is_self_destruct, false) AS is_self_destruct
  , c.creation_tx_hash
FROM cleanup AS c
LEFT JOIN {{ source('ovm1_optimism', 'contracts') }} AS ovm1c
  ON c.contract_address = ovm1c.contract_address --fill in any missing contract creators
LEFT JOIN {{ ref('contracts_optimism_project_name_mappings') }} AS dnm -- fix names for decoded contracts
  ON lower(c.contract_project) = lower(dnm.dune_name)
LEFT JOIN {{ ref('contracts_optimism_contract_overrides') }} AS co --override contract maps
  ON lower(c.contract_address) = lower(co.contract_address)
