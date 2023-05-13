{{ config(
        schema = 'aave_v3'
        , alias = 'tokens'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'atoken_address']
        , post_hook='{{ expose_spells(\'["optimism","polygon","arbitrum","avalanche_c"]\',
                                  "project",
                                  "aave_v3",
                                  \'["msilb7"]\') }}'
  )
}}

-- chains where aave v3 contracts are decoded.
{% set aave_v3_decoded_chains = [
    'optimism',
    'polygon',
    'arbitrum',
    'avalanche_c'
] %}

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
    {% for aave_v3_chain in aave_v3_decoded_chains %}
        SELECT
            '{{ aave_v3_chain }}' AS blockchain,
            contract_address AS atoken_address,
            underlyingasset AS underlying_address,
            atokendecimals AS atoken_decimals,
            'Supply' AS side,
            'Variable' AS arate_type,
            atokensymbol AS atoken_symbol,
            atokenname AS atoken_name
        FROM {{ source( 'aave_v3_' + aave_v3_chain, 'AToken_evt_Initialized' ) }}
        {% if is_incremental() %}
            WHERE evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}

        UNION ALL

        SELECT
            '{{ aave_v3_chain }}' AS blockchain,
            contract_address AS atoken_address,
            underlyingasset AS underlying_address,
            debttokendecimals AS atoken_decimals,
            'Borrow' AS side,
            'Stable' AS arate_type,
            debttokensymbol AS atoken_symbol,
            debttokenname AS atoken_name
        FROM {{ source( 'aave_v3_' + aave_v3_chain, 'StableDebtToken_evt_Initialized' ) }}
        WHERE
            debttokenname LIKE '%Stable%'
            {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}

        UNION ALL

        SELECT
            '{{ aave_v3_chain }}' AS blockchain,
            contract_address AS atoken_address,
            underlyingasset AS underlying_address,
            debttokendecimals AS atoken_decimals,
            'Borrow' AS side,
            'Variable' AS arate_type,
            debttokensymbol AS atoken_symbol,
            debttokenname AS atoken_name
        FROM {{ source( 'aave_v3_' + aave_v3_chain, 'VariableDebtToken_evt_Initialized' ) }}
        WHERE
            debttokenname LIKE '%Variable%'
            {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}

        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
) AS a
