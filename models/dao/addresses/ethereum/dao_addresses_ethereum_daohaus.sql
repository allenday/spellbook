{{ config(
    alias = 'addresses_ethereum_daohaus',
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set moloch_start_date = '2021-01-25' %}

{% set minion_start_date = '2021-09-17' %}

WITH  -- dune query here - https: / /dune.com/queries/1433790

get_daohaus_molochs AS ( -- molochs are daos AND this is getting a list of molochs created through daohaus
        SELECT
            block_time AS created_block_time,
            TRY_CAST(date_trunc('day', block_time) AS DATE) AS created_date,
            CONCAT('0x', RIGHT(topic2, 40)) AS moloch
        FROM
        {{ source('ethereum', 'logs') }}
        {% if NOT is_incremental() %}
        WHERE block_time >= '{{moloch_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0x099e0b09e056ad33e22e4d35de2e837a30ba249f33d912abb7e1e273bbf9d650' -- summon moloch event which is the event emitted when a moloch is created through daohaus
        AND contract_address = '0x38064f40b20347d58b326e767791a6f79cdeddce' -- dao haus moloch v2.1 contract address
),

get_minion_creations AS ( -- minions are created BY molochs to manage funds (this is a gnosis safe that's controlled with zodiac's reality.eth module)
        SELECT
            CONCAT('0x', RIGHT(topic3, 40)) AS moloch,
            CONCAT('0x', RIGHT(topic2, 40)) AS wallet_address
        FROM
        {{ source('ethereum', 'logs') }}
        {% if NOT is_incremental() %}
        WHERE block_time >= '{{minion_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND topic1 = '0xbaefe449c0963ab3bd87eb56115a3f8420fbefae45878f063cc59a6cb99d3ae0' -- summon minion event which is emitted when a minion is created through dao haus
        AND contract_address IN ('0x594af060c08eea9f559bc668484e50596bcb2cfb', '0xbc37509a283e2bb67fd151c34e72e826c501e108') -- dao haus minion summoner contract addresses
),

get_daohaus_wallets AS (
        SELECT
            gm.created_date,
            gm.created_block_time,
            gm.moloch AS dao,
            gm.moloch AS dao_wallet,
            gc.wallet_address AS minion_wallet
        FROM
        get_daohaus_molochs gm
        LEFT JOIN -- getting minions mapped to molochs (using a LEFT JOIN since NOT all molochs have a minion)
        get_minion_creations gc
            ON gm.moloch = gc.moloch
),

mapped_wallets AS (
        SELECT
            'ethereum' AS blockchain,
            'dao-haus' AS dao_creator_tool,
            dao,
            dao_wallet AS dao_wallet_address,
            created_block_time,
            created_date
        FROM
        get_daohaus_wallets

        UNION -- molochs are wallet addresses AS well so using a union here since there'll be duplicates AS i'm unioning the moloch addresses & minion addresses

        SELECT
            'ethereum' AS blockchain,
            'dao-haus' AS dao_creator_tool,
            dao,
            minion_wallet AS dao_wallet_address,
            created_block_time,
            created_date
        FROM
        get_daohaus_wallets
)

SELECT
    DISTINCT(mw.*) -- there are still duplicates so I'm using a distinct to filter for the duplicates
FROM
mapped_wallets mw
WHERE dao_wallet_address IS NOT NULL 