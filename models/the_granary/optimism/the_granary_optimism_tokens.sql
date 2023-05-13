{{ config(
    alias='tokens'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'atoken_address']
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "the_granary",
                                  \'["msilb7"]\') 
    }}'
  )
}}


SELECT DISTINCT
    a.blockchain,
    a.atoken_address,
    a.underlying_address,
    a.atoken_decimals,
    a.side,
    a.arate_type,
    a.atoken_symbol,
    a.atoken_name

FROM (
    SELECT
        'optimism' AS blockchain,
        contract_address AS atoken_address,
        underlyingasset AS underlying_address,
        atokendecimals AS atoken_decimals,
        'Supply' AS side,
        'Variable' AS arate_type,
        atokensymbol AS atoken_symbol,
        atokenname AS atoken_name
    FROM {{ source( 'the_granary_optimism', 'AToken_evt_Initialized' ) }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        'optimism' AS blockchain,
        contract_address AS atoken_address,
        underlyingasset AS underlying_address,
        debttokendecimals AS atoken_decimals,
        'Borrow' AS side,
        CASE WHEN
            debttokenname
            LIKE '%Stable%' THEN 'Stable'
        ELSE 'Variable' END AS arate_type,
        debttokensymbol AS atoken_symbol,
        debttokenname AS atoken_name
    FROM {{ source( 'the_granary_optimism', 'DebtToken_evt_Initialized' ) }}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
) AS a
