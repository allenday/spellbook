{{ config( alias='aggregators') }}

SELECT
    contract_address
    , name
FROM
    (
        VALUES
        -- Element NFT Marketplace Aggregator
        ('0xb3e808e102ac4be070ee3daac70672ffc7c1adca', 'Element')
    )
