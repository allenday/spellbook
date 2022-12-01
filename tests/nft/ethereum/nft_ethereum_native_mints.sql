-- Check if all ENS mints are taken into account
WITH
ens_mints_ctn AS (
    SELECT
        'hi_im_a_dummy' AS dummy,
        count(*) AS ctn
    FROM
        ethereum.logs
    WHERE
        1 = 1
        -- contract = ENS
        AND contract_address = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'
        -- event type = transfer
        -- (this only considers erc721, but that's ok as ENS uses erc721)
        AND topic1 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        -- seller = null address
        AND topic2 = '0x0000000000000000000000000000000000000000000000000000000000000000'
        AND block_time < now() - INTERVAL '1 day' -- allow some head desync

        {% if is_incremental() %}
            AND block_time >= date_trunc('day', now() - INTERVAL '1 week')
        {% endif %}
),

eth_native_mints_ctn AS (
    SELECT
        'hi_im_a_dummy' AS dummy,
        count(*) AS ctn
    FROM
        {{ ref('nft_ethereum_native_mints') }}
    WHERE
        1 = 1
        -- ENS
        AND nft_contract_address = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85'
        -- allow some head desync
        AND block_time < now() - INTERVAL '1 day'
)

SELECT *
FROM
    ens_mints_ctn
INNER JOIN
    eth_native_mints_ctn ON ens_mints_ctn.dummy = eth_native_mints_ctn.dummy
WHERE
    -- pass test when difference in result rows is less than 0.01%
    -- i.e., 0 rows with c1.ctn / c2.ctn larger than  0.01%
    (1 - (ens_mints_ctn.ctn / eth_native_mints_ctn.ctn)) > 0.0001
