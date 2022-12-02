{{ config( alias='aggregators') }}

SELECT
    contract_address
    , name
FROM
    (
        VALUES
        -- Element NFT Marketplace Aggregator
        ('0x37ad2bd1e4f1c0109133e07955488491233c9372', 'Element')
    )
