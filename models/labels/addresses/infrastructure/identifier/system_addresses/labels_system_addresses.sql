{{config(
    alias='system_addresses'
)}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM UNNEST(ARRAY<STRUCT<blockchain STRING,address STRING,name STRING,category STRING,contributor STRING,source STRING,created_at TIMESTAMP,updated_at TIMESTAMP,model_name STRING,label_type STRING>> [STRUCT('optimism', '0x420000000000000000000000000000000000000f',   'Optimism - L1 Gas Price Oracle',   'infrastructure',   'msilb7',   'static',   timestamp('2022-12-02'),    CURRENT_TIMESTAMP(), 'system_addresses', 'identifier'),
STRUCT('arbitrum', '0x00000000000000000000000000000000000a4b05',   'Arbitrum - ArbOS L1 Data Oracle',        'infrastructure',   'msilb7',   'static',   timestamp('2022-12-02'),    CURRENT_TIMESTAMP(), 'system_addresses', 'identifier'),
STRUCT('solana',   'Vote111111111111111111111111111111111111111',  'Solana - Voting Address',          'infrastructure',   'msilb7',   'static',   timestamp('2022-12-02'),    CURRENT_TIMESTAMP(), 'system_addresses', 'identifier'),
STRUCT('optimism', '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001',   'Optimism - L1 Attributes Depositor Contract',        'infrastructure',   'msilb7',   'static',   timestamp('2022-12-28'),    CURRENT_TIMESTAMP(), 'system_addresses', 'identifier'),
STRUCT('optimism', '0x4200000000000000000000000000000000000015',   'Optimism - L1 Attributes Predeployed Contract',        'infrastructure',   'msilb7',   'static',   timestamp('2022-12-28'),    CURRENT_TIMESTAMP(), 'system_addresses', 'identifier')])