{{ config(
    alias = 'addresses_polygon_syndicate',
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set project_start_date = '2022-03-10' %}

WITH -- dune query here  https: / /dune.com/queries/1527974

all_syndicate_daos AS (
        SELECT
            evt_block_time AS block_time,
            tokenAddress AS dao
        FROM {{ source('syndicate_v2_polygon', 'ERC20ClubFactory_evt_ERC20ClubCreated') }}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}

        UNION ALL

        SELECT
            evt_block_time AS block_time,
            tokenAddress AS dao
        FROM {{ source('syndicate_v2_polygon', 'PolygonClubFactoryMATIC_evt_ERC20ClubCreated') }}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}

        UNION ALL

        SELECT
            evt_block_time AS block_time,
            tokenAddress AS dao
        FROM {{ source('syndicate_v2_polygon', 'PolygonERC20ClubFactory_evt_ERC20ClubCreated') }}
        {% if NOT is_incremental() %}
        WHERE evt_block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}
),

ownership_transferred AS ( -- whenever an investment club is created, the ownership can be transferred to another wallet, this happens often AS ownership is transferred to a gnosis safe
        SELECT
            contract_address AS dao,
            block_time,
            CONCAT('0x', RIGHT(topic3, 40)) AS wallet_address
        FROM
        {{ source('polygon', 'logs') }}
        {% if NOT is_incremental() %}
        WHERE block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - INTERVAL '1 week')
        {% endif %}
        AND topic1 = '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0' -- ownership transferred event
        AND contract_address IN (SELECT dao FROM all_syndicate_daos)
),

latest_wallet AS (
        SELECT
            RANK() OVER (PARTITION BY dao ORDER BY block_time DESC) AS change_rank, -- using this to get the most recent owner of the investment club
            dao,
            wallet_address
        FROM
        ownership_transferred
),

syndicate_wallets AS (
        SELECT
            date_trunc('day', ad.block_time) AS created_date,
            ad.block_time AS created_block_time,
            ad.dao,
            lw.wallet_address AS dao_wallet_address
        FROM
        all_syndicate_daos ad
        INNER JOIN -- joining to get the investment club mapped with the owner of the investment club
        latest_wallet lw
            ON ad.dao = lw.dao
        WHERE lw.change_rank = 1 -- getting the most recent owner
)

SELECT
    'polygon' AS blockchain,
    'syndicate' AS dao_creator_tool,
    dao,
    dao_wallet_address,
    created_block_time,
    TRY_CAST(created_date AS DATE) AS created_date
FROM syndicate_wallets
WHERE dao_wallet_address NOT IN ('0xae6328c067bddfba4963e2a1f52baaf11a2e2588', '0x3902ab762a94b8088b71ee5c84bc3c7d2075646b', '0xc08bc955da8968327405642d65a7513ce5eb31ed') -- these are syndicate contract addresses, there's a transfer FROM 0x00...0000 to these addresses during set up so filtering to get rid of them.

