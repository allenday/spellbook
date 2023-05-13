{{ config(
    schema = 'foundation_ethereum',
    alias ='base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}
{% set project_start_date='2021-2-04' %}

WITH all_foundation_trades AS (
    SELECT
        f.evt_block_time AS block_time,
        f.evt_block_number AS block_number,
        c.tokenid AS nft_token_id,
        'Auction Settled' AS trade_category,
        CASE WHEN (f.sellerrev = 0 AND cast(f.creatorrev AS decimal(38)) > 0) THEN 'primary' ELSE 'secondary' END AS trade_type,
        f.seller,
        f.bidder AS buyer,
        f.creatorrev + f.totalfees + f.sellerrev AS price_raw,
        f.contract_address AS project_contract_address,
        c.nftcontract AS nft_contract_address,
        f.evt_tx_hash AS tx_hash,
        f.totalfees AS platform_fee_amount_raw,
        CASE WHEN (f.sellerrev = 0 AND cast(f.creatorrev AS decimal(38)) > 0) THEN 0 ELSE f.creatorrev END AS royalty_fee_amount_raw,
        f.evt_index AS sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_ReserveAuctionFinalized') }} AS f
    INNER JOIN {{ source('foundation_ethereum','market_evt_ReserveAuctionCreated') }} AS c ON c.auctionid = f.auctionid AND c.evt_block_time <= f.evt_block_time
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
        WHERE f.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% else %}
     WHERE f.evt_block_time >= '{{ project_start_date }}'
     {% endif %}
    UNION ALL
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        tokenid AS nft_token_id,
        'Buy' AS trade_category,
        CASE WHEN (sellerrev = 0 AND cast(creatorrev AS decimal(38)) > 0) THEN 'primary' ELSE 'secondary' END AS trade_type,
        seller,
        buyer,
        creatorrev + totalfees + sellerrev AS price_raw,
        contract_address AS project_contract_address,
        nftcontract AS nft_contract_address,
        evt_tx_hash AS tx_hash,
        totalfees AS platform_fee_amount_raw,
        CASE WHEN (sellerrev = 0 AND cast(creatorrev AS decimal(38)) > 0) THEN 0 ELSE creatorrev END AS royalty_fee_amount_raw,
        evt_index AS sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_BuyPriceAccepted') }} AS f
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
        WHERE f.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% else %}
     WHERE f.evt_block_time >= '{{ project_start_date }}'
     {% endif %}
    UNION ALL
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        tokenid AS nft_token_id,
        'Sell' AS trade_category,
        CASE WHEN (sellerrev = 0 AND cast(creatorrev AS decimal(38)) > 0) THEN 'primary' ELSE 'secondary' END AS trade_type,
        seller,
        buyer,
        creatorrev + totalfees + sellerrev AS price_raw,
        contract_address AS project_contract_address,
        nftcontract AS nft_contract_address,
        evt_tx_hash AS tx_hash,
        totalfees AS platform_fee_amount_raw,
        CASE WHEN (sellerrev = 0 AND cast(creatorrev AS decimal(38)) > 0) THEN 0 ELSE creatorrev END AS royalty_fee_amount_raw,
        evt_index AS sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_OfferAccepted') }} AS f
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
        WHERE f.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% else %}
     WHERE f.evt_block_time >= '{{ project_start_date }}'
     {% endif %}
    UNION ALL
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        tokenid AS nft_token_id,
        'Private Sale' AS trade_category,
        CASE WHEN (sellerrev = 0 AND cast(creatorfee AS decimal(38)) > 0) THEN 'primary' ELSE 'secondary' END AS trade_type,
        seller,
        buyer,
        creatorfee + protocolfee + sellerrev AS price_raw,
        contract_address AS project_contract_address,
        nftcontract AS nft_contract_address,
        evt_tx_hash AS tx_hash,
        protocolfee AS platform_fee_amount_raw,
        CASE WHEN (sellerrev = 0 AND cast(creatorfee AS decimal(38)) > 0) THEN 0 ELSE creatorfee END AS royalty_fee_amount_raw,
        evt_index AS sub_tx_trade_id
    FROM {{ source('foundation_ethereum','market_evt_PrivateSaleFinalized') }} AS f
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
        WHERE f.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% else %}
     WHERE f.evt_block_time >= '{{ project_start_date }}'
     {% endif %}
)

SELECT
    date_trunc('day', t.block_time) AS block_date,
    t.block_time,
    t.block_number,
    t.nft_token_id,
    1 AS nft_amount,
    t.trade_category,
    t.trade_type,
    t.seller,
    t.buyer,
    cast(t.price_raw AS decimal(38)) AS price_raw,
    '{{ var("ETH_ERC20_ADDRESS") }}' AS currency_contract, -- all trades are in ETH
    t.project_contract_address,
    t.nft_contract_address,
    t.tx_hash,
    cast(t.platform_fee_amount_raw AS decimal(38)) AS platform_fee_amount_raw,
    cast(t.royalty_fee_amount_raw AS decimal(38)) AS royalty_fee_amount_raw,
    cast(NULL AS string) AS royalty_fee_address,
    cast(NULL AS string) AS platform_fee_address,
    t.sub_tx_trade_id
FROM all_foundation_trades AS t
