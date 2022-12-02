{{config(alias='flashbots_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT DISTINCT
    account_address AS address
    , 'Flashbots User' AS name
    , 'flashbots' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , array('ethereum') AS blockchain
    , date('2022-10-08') AS created_at
    , now() AS modified_at
FROM {{ source('flashbots', 'arbitrages') }}
