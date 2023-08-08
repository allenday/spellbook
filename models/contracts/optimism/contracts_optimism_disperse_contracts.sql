{{ config(
    alias = 'disperse_contracts'
    )
}}



WITH disperse_contracts AS (
    SELECT LOWER(address) AS contract_address, contract_name
    FROM UNNEST(ARRAY<STRUCT<address STRING,contract_name STRING>> [STRUCT('0xd152f549545093347a162dce210e7293f1452150','Disperse') ,('0x52da4de336f8354f7b33a472bf010cd4a3b640ae','Disperse') ,('0x50a64d05bb8618d8d96a83cbbb12b3044ec3489a','Disperse') ,('0x77fb265dac16ccc5cc80c3583a72cf7776f9b759','Disperse') ,('0xbd39115fc389b9a063af22679b49d035e1f1de58','MultiSend') ,('0xdd29ddac5e6ada1359fc20b8debad2b98963e0dd','MultiSend') ,('0x714dc96eb217b511a882b6c472d106620ec5a4d2','MultiSend') ,('0xbe9a9b1b07f027130e56d8569d1aea5dd5a86013','OP Airdrop 2 Distributor')])
    )

SELECT
    contract_address, contract_name

 FROM disperse_contracts