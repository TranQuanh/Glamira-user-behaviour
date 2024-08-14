SELECT 
   option.option_id AS diamond_id,
  option.value_label,
  option.value_id
from {{ref("stg_behaviour")}} behaviour,
UNNEST(option) AS option 
WHERE option.option_id IS NOT NULL and LOWER(option.option_label) ="diamond"
UNION DISTINCT
SELECT 
   cart_products_option.option_id AS diamond_id,
  cart_products_option.value_label,
  CAST(cart_products_option.value_id AS STRING) AS value_id
FROM  {{ref("stg_behaviour")}} behaviour,
UNNEST(cart_products) AS cart_products,
UNNEST(cart_products.option) AS cart_products_option
WHERE cart_products_option.option_id IS NOT NULL AND LOWER(cart_products_option.option_label) ="diamond"
