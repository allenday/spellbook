{{ config( alias='aggregators') }}

SELECT
    contract_address
    , name
FROM
    (
        VALUES
        -- Element NFT Marketplace Aggregator
        ('0x56085ea9c43dea3c994c304c53b9915bff132d20', 'Element')
    )
