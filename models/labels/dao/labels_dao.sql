{{config(alias='dao')}}

SELECT
    array(blockchain) AS blockchain
    , dao AS address
    , CASE
        WHEN dao_creator_tool = 'aragon' THEN 'DAO: Aragon'
        WHEN dao_creator_tool = 'colony' THEN 'DAO: Colony'
        WHEN dao_creator_tool = 'dao-haus' THEN 'DAO: DAO Haus'
        WHEN
            dao_creator_tool = 'syndicate' THEN 'DAO: Syndicate Investment Club'
    END AS name
    , 'daos' AS category
    , 'henrystats' AS contributor
    , 'query' AS source
    , timestamp('2022-11-05') AS created_at
    , now() AS updated_at
FROM
    {{ ref('dao_addresses') }}
-- excluding zodiac since they're gnosis safes
WHERE dao_creator_tool != 'zodiac'

UNION  -- using a UNION because there are daos whose contract address also receives AND send funds

SELECT
    array(blockchain) AS blockchain
    , dao_wallet_address AS address
    , CASE
        WHEN dao_creator_tool = 'aragon' THEN 'DAO: Aragon'
        WHEN dao_creator_tool = 'colony' THEN 'DAO: Colony'
        WHEN dao_creator_tool = 'dao-haus' THEN 'DAO: DAO Haus'
    END AS name
    , 'daos' AS category
    , 'henrystats' AS contributor
    , 'query' AS source
    , timestamp('2022-11-05') AS created_at
    , now() AS updated_at
FROM
    {{ ref('dao_addresses') }}
-- excluding syndicate since their wallet addresses are controlled BY EOAs
WHERE dao_creator_tool NOT IN ('zodiac', 'syndicate')
                                                     -- excluding zodiac since they're gnosis safes
