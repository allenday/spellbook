{{ config(
        alias ='metadata',
        unique_key='punk_id'
        )
}}

select punk_id, punk_type, attribute_count, attribute_list
FROM
  {{ ref( 'cryptopunks_ethereum_metadata_0_seed' ) }}