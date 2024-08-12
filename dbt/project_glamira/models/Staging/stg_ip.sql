WITH ip_source AS(
    SELECT *
    FROM `project-glamira.Glamira.ip`
),
ip_rename AS(
    SELECT
        ip as ip_key,
        country_code,
        country_name,
        region_name,
        city_name,
        latitude,
        longtitude,
        ZIP_code as zip_code,
        time_zone as time_zone
    FROM ip_source
    where ip !="unknown"
),
ip__excecuted_NULL AS(
    SELECT
        ip_key,
        NULLIF(country_code,"-") as country_code,
        NULLIF(country_name,"-") as country_name,
        NULLIF(region_name,"-") as region_name,
        NULLIF(city_name,"-") as city_name,
        latitude,
        longtitude,
        NULLIF(zip_code,"-") as zip_code ,
        NULLIF(time_zone,"-") as time_zone 
    FROM ip_rename
),
ip__cast_type AS(
    SELECT
        ip_key,
        country_code,
        country_name,
        region_name,
        city_name,
        CAST(latitude AS DECIMAL) AS latitude,
        CAST(longtitude AS DECIMAL) AS longtitude,
        zip_code  as zip_code,
        time_zone as time_zone
    FROM ip__excecuted_NULL
)
SELECT *
FROM ip__cast_type
UNION ALL
SELECT
    "0" as ip_key,
    "Undefind" as country_code,
    "Undefind" as country_name,
    "Undefind" as region_name,
    "Undefind" as city_name,
    999 as latitude,
    999 as longtitude,
    "0" as zip_code,
    "Undefind" as time_zone
UNION ALL
SELECT
    "-1" as ip_key,
    "Invalid" as country_code,
    "Invalid" as country_name,
    "Invalid" as region_name,
    "Invalid" as city_name,
    999 as latitude,
    999 as longtitude,
    "-1" as zip_code,
    "Invalid" as time_zone