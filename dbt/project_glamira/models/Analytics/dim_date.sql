WITH dim_date__generate as (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2021-01-01', INTERVAL 1 DAY)) AS date
),

dim_date__enrich as (
SELECT
  date  as date_id,
  FORMAT_DATE("%F",date) as full_date,
  FORMAT_DATE("%A",date) as day_of_week,
  FORMAT_DATE("%a",date) as day_of_week_short,
  DATE_TRUNC(date, MONTH) as year_month,
  FORMAT_DATE("%B" ,date) as month,
  DATE_TRUNC(date ,year) as year,
  EXTRACT(YEAR FROM date) as year_number
FROM
  dim_date__generate
)

SELECT 
*,
CASE 
    WHEN day_of_week_short IN ('Mon','Tue','Wed','Thu','Fri') THEN 'Weekday'
    WHEN day_of_week_short IN ('Sat','Sun') THEN 'Weekend'
    ELSE 'Invalid' 
  END as is_weekday_or_weekend
FROM dim_date__enrich