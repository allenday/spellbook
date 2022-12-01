-- Expose Spells macro:
-- => expose_spells(["blockchains"], 'project' / 'sector', 'name', ["contributors"])
{{
  config(alias='view_bridge_transactions',
         post_hook='{{ expose_spells(\'["ethereum"]\',
                                      "project",
                                      "nomad",
                                    \'["springzh"]\') }}')
}}

with nomad_bridge_domains(domain_id, domain_name, domain_type) AS (
      values
      (6648936, 'Ethereum', 'Outflow'),
      (1650811245, 'Moonbeam', 'Outflow'),
      (70901803, 'Moonbeam', 'Inflow'),
      (1702260083, 'Evmos', 'Outflow'),
      (73111513, 'Evmos', 'Inflow'),
      (25393, 'Milkomeda C1', 'Outflow'),
      (10906210, 'Milkomeda C1', 'Inflow'),
      (1635148152, 'Avalanche', 'Outflow'),
      (70229078, 'Avalanche', 'Inflow'),
      (2019844457, 'Gnosis Chain (xdai)', 'Outflow')
)

,nomad_bridge_transactions AS (
      SELECT evt_block_time AS block_time
          ,evt_block_number AS block_number
          ,evt_tx_hash AS tx_hash
          ,evt_index
          ,'Send' AS transaction_type
          ,s.contract_address AS contract_address
          ,token AS token_address
          ,amount AS original_amount_raw
          ,amount / pow(10, e1.decimals) AS original_amount
          ,e1.symbol AS original_currency
          ,amount / pow(10, e1.decimals) * coalesce(p1.price, 0) AS usd_amount
          ,`FROM` AS sender
          ,concat('0x', right(toId, 40)) AS recipient
          ,toDomain AS domain_id
          ,d.domain_name AS domain_name
          ,fastLiquidityEnabled AS fast_liquidity_enabled
          ,'0x0000000000000000000000000000000000000000' AS liquidity_provider
      FROM {{ source('nomad_ethereum', 'BridgeRouter_evt_Send') }} s
      inner join nomad_bridge_domains d ON d.domain_id = s.toDomain
      LEFT JOIN {{ ref('tokens_erc20') }} e1 ON e1.contract_address = s.token AND e1.blockchain = 'ethereum'
      LEFT JOIN {{ source('prices', 'usd') }} p1 ON p1.contract_address = s.token
            AND p1.minute = date_trunc('minute', s.evt_block_time)
            AND p1.minute >= '2022-01-01'
            AND p1.blockchain = 'ethereum'

      union all

      SELECT evt_block_time AS block_time
          ,evt_block_number AS block_number
          ,evt_tx_hash AS tx_hash
          ,evt_index
          ,'Receive' AS transaction_type
          ,r.contract_address AS contract_address
          ,token AS token_address
          ,amount AS original_amount_raw
          ,amount / pow(10, e1.decimals) AS original_amount
          ,e1.symbol AS original_currency
          ,amount / pow(10, e1.decimals) * coalesce(p1.price, 0) AS usd_amount
          ,t.`FROM` AS sender
          ,r.recipient
          ,left(originAndNonce, 8) AS domain_id
          ,d.domain_name AS domain_name
          ,false AS fast_liquidity_enabled
          ,liquidityProvider AS liquidity_provider
      FROM {{ source('nomad_ethereum', 'BridgeRouter_evt_Receive') }} r
      inner join {{ source('ethereum', 'transactions') }} t ON r.evt_block_number = t.block_number
            AND r.evt_tx_hash = t.hash
            AND t.block_time >= '2022-01-01'
      inner join nomad_bridge_domains d ON d.domain_id = left(originAndNonce, 8)
      LEFT JOIN {{ ref('tokens_erc20') }} e1 ON e1.contract_address = r.token AND e1.blockchain = 'ethereum'
      LEFT JOIN {{ source('prices', 'usd') }} p1 ON p1.contract_address = r.token
            AND p1.minute = date_trunc('minute', r.evt_block_time)
            AND p1.minute >= '2022-01-01'
            AND p1.blockchain = 'ethereum'
)

SELECT block_time
      ,block_number
      ,tx_hash
      ,evt_index
      ,transaction_type
      ,contract_address
      ,token_address
      ,original_amount_raw
      ,original_amount
      ,original_currency
      ,usd_amount
      ,sender
      ,recipient
      ,domain_id
      ,domain_name
      ,fast_liquidity_enabled
      ,liquidity_provider
  FROM nomad_bridge_transactions
