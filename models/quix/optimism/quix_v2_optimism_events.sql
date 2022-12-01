{{ config(
    schema = 'quix_v2_optimism',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'token_id', 'seller',  'evt_index']
    )
}}
{% set quix_fee_address_address = "0xec1557a67d4980c948cd473075293204f4d280fd" %}
{% set min_block_number = 2753614 %}


with events_raw AS (
    SELECT
      *
    from (
        SELECT
            evt_block_number AS block_number
            ,tokenId AS token_id
            ,contract_address AS project_contract_address
            ,evt_tx_hash AS tx_hash
            ,evt_block_time AS block_time
            ,buyer
            ,seller
            ,erc721address AS nft_contract_address
            ,price AS amount_raw
        from {{ source('quixotic_v2_optimism','ExchangeV2_evt_BuyOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        union all

        SELECT
            evt_block_number AS block_number
            ,tokenId AS token_id
            ,contract_address AS project_contract_address
            ,evt_tx_hash AS tx_hash
            ,evt_block_time AS block_time
            ,buyer
            ,seller
            ,erc721address AS nft_contract_address
            ,price AS amount_raw
        from {{ source('quixotic_v2_optimism','ExchangeV2_evt_DutchAuctionFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        union all

        SELECT
            evt_block_number AS block_number
            ,tokenId AS token_id
            ,contract_address AS project_contract_address
            ,evt_tx_hash AS tx_hash
            ,evt_block_time AS block_time
            ,buyer
            ,seller
            ,erc721address AS nft_contract_address
            ,price AS amount_raw
        from {{ source('quixotic_v2_optimism','ExchangeV2_evt_SellOrderFilled') }}
        {% if is_incremental() %} -- this filter will only be applied on an incremental run
        where evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ) AS x
    where nft_contract_address != lower('0xbe81eabdbd437cba43e4c1c330c63022772c2520') -- --exploit contract
)
,transfers AS (
    -- eth royalities
    SELECT
      tr.tx_block_number AS block_number
      ,tr.tx_block_time AS block_time
      ,tr.tx_hash
      ,tr.value
      ,tr.to
    from events_raw AS er
    join {{ ref('transfers_optimism_eth') }} AS tr
      on er.tx_hash = tr.tx_hash
      and er.block_number = tr.tx_block_number
      and tr.value_decimal > 0
      and tr.to NOT in (
        lower('{{quix_fee_address_address}}') --qx platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if NOT is_incremental() %}
      -- smallest block number for source tables above
      and tr.tx_block_number >= '{{min_block_number}}'
      {% endif %}
      {% if is_incremental() %}
      and tr.tx_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}

    union all

    -- erc20 royalities
    SELECT
      erc20.evt_block_number AS block_number
      ,erc20.evt_block_time AS block_time
      ,erc20.evt_tx_hash AS tx_hash
      ,erc20.value
      ,erc20.to
    from events_raw AS er
    join {{ source('erc20_optimism','evt_transfer') }} AS erc20
      on er.tx_hash = erc20.evt_tx_hash
      and er.block_number = erc20.evt_block_number
      and erc20.value is NOT NULL
      and erc20.to NOT in (
        lower('{{quix_fee_address_address}}') --qx platform fee address
        ,er.seller
        ,er.project_contract_address
      )
      {% if NOT is_incremental() %}
      -- smallest block number for source tables above
      and erc20.evt_block_number >= '{{min_block_number}}'
      {% endif %}
      {% if is_incremental() %}
      and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
      {% endif %}
)
SELECT
    'optimism' AS blockchain
    ,'quix' AS project
    ,'v2' AS version
    ,TRY_CAST(date_trunc('DAY', er.block_time) AS date) AS block_date
    ,er.block_time
    ,er.token_id
    ,n.name AS collection
    ,er.amount_raw / power(10, t1.decimals) * p1.price AS amount_usd
    ,'erc721' AS token_standard
    ,'Single Item Trade' AS trade_type
    ,cast(1 AS bigint) AS number_of_items
    ,'Buy' AS trade_category
    ,'Trade' AS evt_type
    ,er.seller
    ,case
    when er.buyer = agg.contract_address then erct2.to
    else er.buyer
    end AS buyer
    ,er.amount_raw / power(10, t1.decimals) AS amount_original
    ,er.amount_raw
    ,case
        when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is NULL)
            then 'ETH'
            else t1.symbol
        end AS currency_symbol
    ,case
        when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is NULL)
            then '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
            else erc20.contract_address
        end AS currency_contract
    ,er.nft_contract_address
    ,er.project_contract_address
    ,agg.name AS aggregator_name
    ,agg.contract_address AS aggregator_address
    ,er.tx_hash
    ,coalesce(erct2.evt_index,1) AS evt_index
    ,er.block_number
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,ROUND((2.5*(er.amount_raw) / 100),7) AS platform_fee_amount_raw
    ,ROUND((2.5*((er.amount_raw / power(10,t1.decimals)))/100),7) AS platform_fee_amount
    ,ROUND((2.5*((er.amount_raw / power(10,t1.decimals)* p1.price))/100),7) AS platform_fee_amount_usd
    ,'2.5' AS platform_fee_percentage
    ,tr.value AS royalty_fee_amount_raw
    ,tr.value / power(10, t1.decimals) AS royalty_fee_amount
    ,tr.value / power(10, t1.decimals) * p1.price AS royalty_fee_amount_usd
    ,(tr.value / er.amount_raw * 100) AS royalty_fee_percentage
    ,case when tr.value is NOT NULL then tr.to end AS royalty_fee_receive_address
    ,case when tr.value is NOT NULL
        then case when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is NULL)
            then 'ETH' else t1.symbol end
        end AS royalty_fee_currency_symbol
from events_raw AS er
join {{ source('optimism','transactions') }} AS tx
    on er.tx_hash = tx.hash
    and er.block_number = tx.block_number
    {% if NOT is_incremental() %}
    -- smallest block number for source tables above
    and tx.block_number >= '{{min_block_number}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('nft_aggregators') }} AS agg
    on agg.contract_address = tx.to
    and agg.blockchain = 'optimism'
LEFT JOIN {{ ref('tokens_nft') }} n
    on n.contract_address = er.nft_contract_address
    and n.blockchain = 'optimism'
LEFT JOIN {{ source('erc721_optimism','evt_transfer') }} AS erct2
    on erct2.evt_block_time=er.block_time
    and er.nft_contract_address=erct2.contract_address
    and erct2.evt_tx_hash=er.tx_hash
    and erct2.tokenId=er.token_id
    and erct2.from=er.buyer
    {% if NOT is_incremental() %}
    -- smallest block number for source tables above
    and erct2.evt_block_number >= '{{min_block_number}}'
    {% endif %}
    {% if is_incremental() %}
    and erct2.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('erc20_optimism','evt_transfer') }} AS erc20
    on erc20.evt_block_time=er.block_time
    and erc20.evt_tx_hash=er.tx_hash
    and erc20.to=er.seller
    {% if NOT is_incremental() %}
    -- smallest block number for source tables above
    and erc20.evt_block_number >= '{{min_block_number}}'
    {% endif %}
    {% if is_incremental() %}
    and erc20.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} AS t1
    on t1.contract_address =
        case when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is NULL)
        then '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
        else erc20.contract_address
        end
    and t1.blockchain = 'optimism'
    LEFT JOIN {{ source('prices', 'usd') }} AS p1
    on p1.contract_address =
        case when (erc20.contract_address = '0x0000000000000000000000000000000000000000' or erc20.contract_address is NULL)
        then '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
        else erc20.contract_address
        end
    and p1.minute = date_trunc('minute', er.block_time)
    and p1.blockchain = 'optimism'
    {% if is_incremental() %}
    and p1.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN transfers AS tr
    on tr.tx_hash = er.tx_hash
    and tr.block_number = er.block_number
;