{{ config(
        schema = 'op_token_optimism',
        alias='inflation_schedule'
        , unique_key = ['schedule_confirmed_date', 'schedule_start_date']
        
  )
}}

SELECT 
          cast(schedule_confirmed_date as date) AS schedule_confirmed_date
        , cast(schedule_start_date as date) AS schedule_start_date
        , cast(inflation_rate as FLOAT64) AS inflation_rate
        , inflation_time_period_granularity

FROM UNNEST(ARRAY<STRUCT<schedule_confirmed_date STRING,schedule_start_date STRING,inflation_rate NUMERIC,inflation_time_period_granularity BIGNUMERIC>> [STRUCT('2022-05-31','2023-05-31', 0.02, 'year')])