WITH product_name_source AS(
    SELECT *
    FROM `project-glamira.Glamira.product_name`
),
product_name_rename AS(
    SELECT
        product_id as product_id,
        product_name as product_name
    FROM product_name_source
),
product_name__cast_type AS(
    SELECT 
        CAST(product_id AS INTEGER) AS product_id,
        product_name AS product_name
    FROM product_name_rename
)
SELECT *
FROM product_name__cast_type
UNION ALL
SELECT  
    0 as product_id,
    "Undefind" as product_name
UNION ALL 
SELECT
    -1 as product_id,
    "Invalid" as product_name 
