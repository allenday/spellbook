{{ config(
    alias = 'events',
    partition_by = {"field": "block_date"},
    materialized = 'view',
            unique_key = ['block_date','unique_trade_id']
    )
}}

WITH me_txs AS (
    SELECT
    id,
    pre_balances,
    post_balances,
    block_slot,
    block_date,
    block_time,
    account_keys,
    log_messages,
    instructions,
    signatures,
    signer,
    filter(
        instructions,
        x -> (
            x.executing_account = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
            OR x.executing_account = 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
        )
    ) AS me_instructions
    FROM {{ source('solana','transactions') }}
    WHERE (
         array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K') -- magic eden v2
         OR array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')
    )
    AND success = 'True'
    {% if not is_incremental() %}
    AND block_date > '2022-01-05'
    AND block_slot > 114980355
    {% endif %}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND block_date >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
    {% endif %}

)
SELECT
  'solana' as blockchain,
  'magiceden' as project,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'v2'
  WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'launchpad_v3'
  END as version,
  signatures[0] as tx_hash,
  block_date,
  block_time,
  CAST(block_slot AS BIGINT) as block_number,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price AS amount_usd,
  abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) AS amount_original,
  CAST(abs(post_balances[0] - pre_balances[0]) AS BIGNUMERIC) AS amount_raw,
  p.symbol as currency_symbol,
  p.contract_address as currency_contract,
  'metaplex' as token_standard,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K')) THEN 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
       WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'
       END as project_contract_address,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND (
               array_contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR array_contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
       AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'Trade'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND array_contains(log_messages, 'Program log: Instruction: Sell') THEN 'List'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'Bid'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND array_contains(log_messages, 'Program log: Instruction: CancelBuy') THEN 'Cancel Bid'
  WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
       AND array_contains(log_messages, 'Program log: Instruction: CancelSell') THEN 'Cancel Listing'
  WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'))
       AND array_contains(log_messages, 'Program log: Instruction: SetAuthority') THEN 'Mint'
  ELSE 'Other' END as evt_type,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               array_contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR array_contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN me_instructions[1].account_arguments[2]::string
       WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb'))
         AND array_contains(log_messages, 'Program log: Instruction: SetAuthority') THEN COALESCE(me_instructions[6].account_arguments[9], me_instructions[5].account_arguments[9],
         me_instructions[4].account_arguments[9], me_instructions[2].account_arguments[7], me_instructions[1].account_arguments[10], me_instructions[0].account_arguments[10])::string
       END AS token_id,
  CAST(NULL AS string) as collection,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               array_contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR array_contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'Single Item Trade' ELSE CAST(NULL AS string)
         END as trade_type,
  CAST(1 AS BIGNUMERIC) as number_of_items,
  CAST(NULL AS string) as trade_category,
  signer as buyer,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               array_contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR array_contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
         AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN me_instructions[2].account_arguments[1]::string
       WHEN (array_contains(account_keys, 'CMZYPASGWeTz7RNGHaRJfCq2XQ5pYK6nDvVQxzkH51zb')) THEN '' END as seller,
  CAST(NULL AS string) as nft_contract_address,
  CAST(NULL AS string) as aggregator_name,
  CAST(NULL AS string) as aggregator_address,
  CAST(NULL AS string) as tx_from,
  CAST(NULL AS string) as tx_to,
  2*(abs(post_balances[0] - pre_balances[0])::string)/100 as platform_fee_amount_raw,
  2*(abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9))/100 as platform_fee_amount,
  2*(abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9) * p.price)/100 as platform_fee_amount_usd,
  CAST(2 AS FLOAT64) as platform_fee_percentage,
  CAST (abs(post_balances[11] - pre_balances[11]) + abs(post_balances[12] - pre_balances[12])
    + abs(post_balances[13] - pre_balances[13]) + abs(post_balances[14] - pre_balances[14])  + abs(post_balances[15] - pre_balances[15]) AS FLOAT64) as royalty_fee_amount_raw,
  abs(post_balances[11] / 1e9 - pre_balances[11] / 1e9) + abs(post_balances[12] / 1e9 - pre_balances[12] / 1e9)
    + abs(post_balances[13] / 1e9 - pre_balances[13] / 1e9) + abs(post_balances[14] / 1e9 - pre_balances[14] / 1e9) + abs(post_balances[15] / 1e9 - pre_balances[15] / 1e9)
    as royalty_fee_amount,
  (abs(post_balances[11] / 1e9 - pre_balances[11] / 1e9) + abs(post_balances[12] / 1e9 - pre_balances[12] / 1e9)
    + abs(post_balances[13] / 1e9 - pre_balances[13] / 1e9) + abs(post_balances[14] / 1e9 - pre_balances[14] / 1e9) + abs(post_balances[15] / 1e9 - pre_balances[15] / 1e9)) *
    p.price as royalty_fee_amount_usd,
  ROUND(((abs(post_balances[10] / 1e9 - pre_balances[10] / 1e9)
  +abs(post_balances[11] / 1e9 - pre_balances[11] / 1e9)
  +abs(post_balances[12] / 1e9 - pre_balances[12] / 1e9)
  +abs(post_balances[13] / 1e9 - pre_balances[13] / 1e9)
  +abs(post_balances[14] / 1e9 - pre_balances[14] / 1e9)
  +abs(post_balances[15] / 1e9 - pre_balances[15] / 1e9)) / ((abs(post_balances[0] / 1e9 - pre_balances[0] / 1e9)-0.00204928)) * 100),2) as royalty_fee_percentage,
  CAST(NULL AS FLOAT64) as royalty_fee_receive_address,
  CASE WHEN (array_contains(account_keys, 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'))
         AND (
               array_contains(log_messages, 'Program log: Instruction: ExecuteSaleV2')
               OR array_contains(log_messages, 'Program log: Instruction: ExecuteSale')
               OR array_contains(log_messages, 'Program log: Instruction: Mip1ExecuteSaleV2')
          )
          AND array_contains(log_messages, 'Program log: Instruction: Buy') THEN 'SOL'
         ELSE CAST(NULL AS string) END as royalty_fee_currency_symbol,
  id  as unique_trade_id,
  instructions,
  signatures,
  log_messages
FROM me_txs
LEFT JOIN {{ source('prices', 'usd') }} AS p
  ON p.minute = TIMESTAMP_TRUNC(block_time, minute)
  AND p.blockchain is NULL
  AND p.symbol = 'SOL'
  {% if is_incremental() %}
  AND p.minute >= date_trunc("day", CURRENT_TIMESTAMP() - interval '1 week')
  {% endif %}