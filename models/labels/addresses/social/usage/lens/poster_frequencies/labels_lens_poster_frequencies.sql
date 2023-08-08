{{config(alias='lens_poster_frequencies'

    )
}}

WITH lens_addresses as 

(SELECT to as address
       ,handle as name
       ,CAST (profileId AS string) AS profileId
FROM {{ source('lens_polygon','LensHub_evt_ProfileCreated') }} pc 



)

,post_data as
 (
    SELECT
         output_0 as post_id
         ,CAST(JSON_EXTRACT_SCALAR(vars, '$.profileId') AS string) as profileId
 
    FROM {{ source('lens_polygon','LensHub_call_post') }} cp1
    
 
    WHERE 1=1
    AND call_success = true

    
    union all

    
    SELECT
        output_0 as post_id
        ,CAST(JSON_EXTRACT_SCALAR(vars, '$.profileId') AS string) as profileId
    FROM {{ source('lens_polygon','LensHub_call_postWithSig') }} cp2

  
    WHERE 1=1
    AND call_success = true
)

,post_count as 
(SELECT  distinct profileId 
       ,COUNT(post_id) as posts_count
FROM post_data
GROUP BY 1
)
,percentile as 
(SELECT approx_percentile(posts_count, 0.99) as p99
        ,approx_percentile(posts_count, 0.95) as p95
        ,approx_percentile(posts_count, 0.90) as p90
        ,approx_percentile(posts_count, 0.80) as p80
        ,approx_percentile(posts_count, 0.5) as p50
FROM  post_count
)

SELECT 
'polygon' as blockchain 
,address
,CASE WHEN posts_count >= (select p99 from percentile) THEN 'top MOD( 1, lens) poster'
      WHEN posts_count >= (select p95 from percentile) THEN 'top MOD( 5, lens) poster'
      WHEN posts_count >= (select p90 from percentile) THEN 'top MOD( 10, lens) poster'
      WHEN posts_count >= (select p80 from percentile) THEN 'top MOD( 20, lens) poster'
      WHEN posts_count <= (select p50 from percentile) THEN 'bottom MOD( 50, lens) poster'
      ELSE 'between MOD( 50, to) MOD( 80, lens) posters' END AS name
 ,'social' AS category
 ,'scoffie' AS contributor
 ,'query' AS source
 ,timestamp('2023-03-17') as created_at
 ,CURRENT_TIMESTAMP() as updated_at
 ,'lens_poster_frequencies' as model_name
 ,'usage' as label_type
FROM (
    SELECT 
        address --make distinct
        , sum(posts_count) as posts_count
    FROM lens_addresses la
    INNER JOIN post_count  pc 
    ON la.profileId=pc.profileId 
    GROUP BY 1
)