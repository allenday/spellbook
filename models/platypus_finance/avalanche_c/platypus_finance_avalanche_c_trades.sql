{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "platypus_finance_v1",
                                \'["umer_h_adil"]\') }}'
    )
}}

 / *
    buyer
        gives fromAmount of fromToken
        receives toAmount of toToken
    seller (ie Platypus)
        gives toAmount of toToken,
        receives fromAmount of fromToken
    Platypus allows the sender & reciever to be different
    ie--
        a regular swap looks like--	sender <-> pool
        but Platypus allows--		sender -> pool -> receiver
        here, receiver and sender can be identical (resulting in a regular swap), but don't have to be
    As the receiver (ie the `to` address) ultimately receives the swapped tokens, we designate him / her AS taker
* / 
{% set project_start_date = '2021-11-26' %}

SELECT
	'avalanche_c' AS blockchain
	, 'platypus_finance' AS project
	, '1' AS version
	, date_trunc('DAY', s.evt_block_time) AS block_date
	, s.evt_block_time AS block_time
	, CAST(s.toAmount AS DECIMAL(38,0)) AS token_bought_amount_raw
	, CAST(s.fromAmount AS DECIMAL(38,0)) AS token_sold_amount_raw
    , coalesce(
        (s.toAmount / power(10, prices_b.decimals)) * prices_b.price
        ,(s.fromAmount / power(10, prices_s.decimals)) * prices_s.price
    ) AS amount_usd
	, s.toToken AS token_bought_address
	, s.fromToken AS token_sold_address
	, erc20_b.symbol AS token_bought_symbol
	, erc20_s.symbol AS token_sold_symbol
	, case
        when lower(erc20_b.symbol) > lower(erc20_s.symbol) then concat(erc20_s.symbol, '-', erc20_b.symbol)
        else concat(erc20_b.symbol, '-', erc20_s.symbol)
    end AS token_pair
	, s.toAmount / power(10, erc20_b.decimals) AS token_bought_amount
	, s.fromAmount / power(10, erc20_s.decimals) AS token_sold_amount
    , coalesce(s.`to`, tx.FROM) AS taker
	, '' AS maker
	, cast(s.contract_address AS STRING) AS project_contract_address
	, s.evt_tx_hash AS tx_hash
    , tx.FROM AS tx_from
    , tx.to AS tx_to
	, '' AS trace_address
	, s.evt_index AS evt_index
FROM
    {{ source('platypus_finance_avalanche_c', 'Pool_evt_Swap') }} s
inner join {{ source('avalanche_c', 'transactions') }} tx
    ON tx.hash = s.evt_tx_hash
    {% if NOT is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time = date_trunc("day", now() - interval '1 week')
    {% endif %}
-- bought tokens
LEFT JOIN {{ ref('tokens_erc20') }} erc20_b
    on erc20_b.contract_address = s.toToken
    and erc20_b.blockchain = 'avalanche_c'
-- sold tokens
LEFT JOIN {{ ref('tokens_erc20') }} erc20_s
    on erc20_s.contract_address = s.fromToken
    and erc20_s.blockchain = 'avalanche_c'
-- price of bought tokens
LEFT JOIN {{ source('prices', 'usd') }} prices_b
    on prices_b.minute = date_trunc('minute', s.evt_block_time)
    and prices_b.contract_address = s.toToken
    and prices_b.blockchain = 'avalanche_c'
	{% if NOT is_incremental() %}
    and prices_b.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and prices_b.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
-- price of sold tokens
LEFT JOIN {{ source('prices', 'usd') }} prices_s
    on prices_s.minute = date_trunc('minute', s.evt_block_time)
    and prices_s.contract_address = s.fromToken
    and prices_s.blockchain = 'avalanche_c'
	{% if NOT is_incremental() %}
    and prices_s.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and prices_s.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
where 1 = 1
    {% if is_incremental() %}
    and s.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
;