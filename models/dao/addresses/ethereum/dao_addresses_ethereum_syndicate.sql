{{ config(
    alias = 'addresses_ethereum_syndicate',
    partition_by = ['created_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['created_block_time', 'dao_wallet_address', 'blockchain', 'dao', 'dao_creator_tool']
    )
}}

{% set project_start_date = '2022-01-25' %}

WITH -- dune query here  https://dune.com/queries/1479702

syndicatev2_daos AS ( -- decoded event on dune for syndicate v2, this returns investment clubs deployed on syndicate v2 
    SELECT
        evt_block_time AS block_time,
        tokenaddress AS dao
    FROM {{ source('syndicate_v2_ethereum', 'ERC20ClubFactory_evt_ERC20ClubCreated') }}
    {% if not is_incremental() %}
        WHERE evt_block_time >= '{{ project_start_date }}'
        {% endif %}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
),

syndicatev1_daos AS ( -- getting investment clubs created on dune v1 
    SELECT
        block_time,
        CONCAT("0x", RIGHT(topic2, 40)) AS dao
    FROM
        {{ source('ethereum', 'logs') }}
    {% if not is_incremental() %}
        WHERE block_time >= '{{ project_start_date }}'
        {% endif %}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
        AND topic1 = "0x5fb43bb458994f457693950959aff3a8815320c4da2fb30aa66137420808172e" -- syndicate investment club created event 
),

all_syndicate_daos AS ( -- joining both tables above 
    SELECT * FROM syndicatev2_daos
    UNION ALL
    SELECT * FROM syndicatev1_daos
),

ownership_transferred AS ( -- whenever an investment club is created, the ownership can be transferred to another wallet, this happens often as ownership is transferred to a gnosis safe
    SELECT
        contract_address AS dao,
        block_time,
        CONCAT("0x", RIGHT(topic3, 40)) AS wallet_address
    FROM
        {{ source('ethereum', 'logs') }}
    {% if not is_incremental() %}
        WHERE block_time >= '{{ project_start_date }}'
        {% endif %}
    {% if is_incremental() %}
        WHERE block_time >= date_trunc("day", now() - interval "1 week")
    {% endif %}
        AND topic1 = "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0" -- ownership transferred event 
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
        date_trunc("day", ad.block_time) AS created_date,
        ad.block_time AS created_block_time,
        ad.dao,
        lw.wallet_address AS dao_wallet_address
    FROM
        all_syndicate_daos AS ad
    INNER JOIN -- joining to get the investment club mapped with the owner of the investment club
        latest_wallet AS lw
        ON ad.dao = lw.dao
    WHERE lw.change_rank = 1 -- getting the most recent owner 
)

SELECT
    "ethereum" AS blockchain,
    "syndicate" AS dao_creator_tool,
    dao,
    dao_wallet_address,
    created_block_time,
    TRY_CAST(created_date AS date) AS created_date
FROM syndicate_wallets
WHERE dao_wallet_address NOT IN ("0x2da762e665fe9c220f7011d4ee9c2d15aaa27f9d", "0x2372fd8d69da29b4b328b518c6d7e84f3aa25dc3", "0x99116a5641dc89a7cb43a9a82694177538aa0391", "0x22a3d80299d4f2437611e1ca0b7c8d50f4816c6e") -- these are syndicate contract addresses, there's a transfer from 0x00...0000 to these addresses during set up so filtering to get rid of them. 
