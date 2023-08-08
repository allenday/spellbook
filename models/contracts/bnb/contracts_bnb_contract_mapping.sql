{{
  config(
        alias='contract_mapping',
        tags=['static']
        )
}}

select trim(lower(contract_address)) as contract_address
     , trim(project_name)            as project_name
     , trim(project_type)            as project_type
FROM
  {{ ref( 'contracts_bnb_contract_mapping_0_seed' ) }}