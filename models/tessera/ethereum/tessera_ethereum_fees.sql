{{ config (
    alias = 'fees'
) }}
-- FEES GENERATED
WITH lpda_fees AS (
    SELECT
        _vault AS vault,
        _receiver AS receiver,
        'LPDA' AS source,
        CAST(_amount AS FLOAT64)/POWER(10, 18) AS amount,
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash
    FROM
        {{ source('tessera_ethereum','LPDA_evt_FeeDispersed') }}
)

SELECT *
FROM lpda_fees;
-- UNION ALL with future sources