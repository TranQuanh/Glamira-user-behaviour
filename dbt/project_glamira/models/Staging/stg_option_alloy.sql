WITH stg_alloy_source AS(
    SELECT 
    option.option_id,
    option.value_label,
    option.value_id,
    from {{ref("stg_behaviour")}} behaviour,
    UNNEST(option) AS option 
    WHERE option.option_id IS NOT NULL and LOWER(option.option_label) ="alloy"
    UNION DISTINCT
    SELECT 
    cart_products_option.option_id,
    cart_products_option.value_label,
    CAST(cart_products_option.value_id AS STRING) AS value_id
    FROM  {{ref("stg_behaviour")}} behaviour,
    UNNEST(cart_products) AS cart_products,
    UNNEST(cart_products.option) AS cart_products_option
    WHERE cart_products_option.option_id IS NOT NULL AND LOWER(cart_products_option.option_label) ="alloy"
),
stg_alloy_convert AS(
    SELECT
        FARM_FINGERPRINT(value_label) as alloy_id,
        value_label,
    FROM stg_alloy_source
    WHERE value_label IS NOT NULL AND option_id IS NOT NULL
    GROUP BY value_label
)
SELECT *
FROM stg_alloy_convert
UNION ALL
SELECT
    -2 as alloy_id,
    "Not include" as value_label
