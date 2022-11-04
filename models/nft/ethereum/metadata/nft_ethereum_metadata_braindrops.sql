{{ config(
        alias = 'braindrops'
        )
}}

select contract_address, project_id, project_id_base_value, collection_name, artist_name, art_collection_unique_id
{% if var('declare_values_with_unnest') %}
FROM UNNEST([
STRUCT<contract_address STRING, project_id INT64, project_id_base_value INT64, collection_name STRING, artist_name STRING, art_collection_unique_id STRING>
{% else %}
FROM (VALUES
{% endif %}
        ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 1, 1000000, 'Brain Loops', 'Gene Kogan', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-1')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 2, 1000000, 'podGANs', 'Pindar Van Arman', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-2')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 3, 1000000, 'Genesis', 'Claire Silver', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-3')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 4, 1000000, 'DreamScapes', 'Xander Steenbrugge', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-4')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 5, 1000000, 'Confluence', 'Devi Parikh', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-5')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 6, 1000000, '🎵 Fake Feelings', 'Dadabots x Silverstein', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-6')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 7, 1000000, 'Dream Capsules', 'Obvious', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-7')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 8, 1000000, 'Deep Journeys', 'Heavens Last Angel', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-8')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 9, 1000000, 'TEOPEMA', 'Vadim Epstein x COH', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-9')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 10, 1000000, 'SIGHTS', 'Artemis', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-10')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 11, 1000000, 'ClipMatrix Creatures', 'Nikolay Jetchev', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-11')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 12, 1000000, 'Chimerical Stories', 'Entangled Others (Sofia Crespo & Feileacan McCormick)', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-12')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 13, 1000000, 'ELEMENTS', 'ATNPassion', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-13')
        , ('0xdfde78d2baec499fe18f2be74b6c287eed9511d7', 14, 1000000, 'miniPODs', 'Van Arman x Mindshift x ricky', '0xdfde78d2baec499fe18f2be74b6c287eed9511d7-14')


{% if var('declare_values_with_unnest') %}
])
{% else %}
) AS temp_table (contract_address, project_id, project_id_base_value, collection_name, artist_name, art_collection_unique_id)
{% endif %}

order by project_id asc 
