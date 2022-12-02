{{ config( alias='aggregators') }}

SELECT
    name
    , lower(contract_address) AS contract_address
FROM
    (
        VALUES
        ('0xbbbbbbbe843515689f3182b748b5671665541e58', 'bluesweep') -- bluesweep
    )
