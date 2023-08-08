{{config(alias='cex_fantom')}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM UNNEST(ARRAY<STRUCT<blockchain STRING,address STRING,name STRING,category STRING,contributor STRING,source STRING,created_at TIMESTAMP,updated_at TIMESTAMP,model_name STRING,label_type BIGNUMERIC>> [STRUCT('fantom', '0x8e1701cfd85258ddb8dfe89bc4c7350822b9601d', 'MEXC: Hot Wallet', 'institution', 'Henrystats', 'static', timestamp('2023-01-27'), CURRENT_TIMESTAMP(), 'cex_fantom', 'identifier')])