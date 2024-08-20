WITH stg_behaviour_checkout_success_source AS(
    SELECT
        behaviour.behaviour_id as log_id,
        TIMESTAMP_SECONDS(behaviour.time_stamp) AS time_stamp,
        behaviour.ip  AS territory_id,
        behaviour.user_agent,
        behaviour.resolution,
        behaviour.user_id_db,
        behaviour.device_id,
        behaviour.api_version,
        behaviour.store_id,
        behaviour.local_time,
        behaviour.show_recommendation,
        behaviour.current_url,
        behaviour.referrer_url,
        behaviour.email_address,
        behaviour.order_id,
        cart_products.product_id,
        cart_products.amount,
        cart_products.price,
        cart_products.currency,
        LOWER(cart_products_option.option_label) as option_label,
        cart_products_option.option_id,
        cart_products_option.value_label
    FROM {{ref("stg_behaviour")}} as behaviour,
    UNNEST(cart_products) as cart_products,
    UNNEST(cart_products.option) as cart_products_option
    WHERE collection = "checkout_success" AND cart_products_option IS NOT NULL
),
stg_behaviour_checkout_success_convert AS(
    SELECT
        log_id,
        DATE(time_stamp) AS date,
        TIME(time_stamp) AS order_time,
        territory_id,
        user_agent,
        resolution,
        user_id_db,
        device_id,
        api_version,
        store_id,
        local_time,
        show_recommendation,
        current_url,
        referrer_url,
        email_address,
        order_id,
        product_id,
        amount,
        price,
        currency,
        CASE
            WHEN option_label = 'diamond' THEN FARM_FINGERPRINT(value_label)
            ELSE NULL
        END AS diamond_id,
        CASE
            WHEN option_label = 'alloy' THEN FARM_FINGERPRINT(value_label)
            ELSE NULL
        END AS alloy_id,
    FROM stg_behaviour_checkout_success_source 
),
stg_behaviour_checkout_success_excecuted_NULL AS(
    SELECT
        log_id,
        date AS date_id,
        order_time,
        territory_id ,
        user_agent,
        resolution,
        user_id_db,
        device_id,
        api_version,
        store_id,
        local_time,
        show_recommendation,
        current_url,
        referrer_url,
        email_address,
        order_id,
        product_id,
        amount,
        price,
        currency,
        MAX(diamond_id) AS diamond_id,
        MAX(alloy_id) AS alloy_id
    FROM stg_behaviour_checkout_success_convert
    GROUP BY 
        log_id,
        date_id,
        order_time,
        territory_id,
        user_agent,
        resolution,
        user_id_db,
        device_id,
        api_version,
        store_id,
        local_time,
        show_recommendation,
        current_url,
        referrer_url,
        email_address,
        order_id,
        product_id,
        amount,
        price,
        currency
),
stg_behaviour_checkout_success_check_undefine AS(
    SELECT
        log_id,
        date_id,
        order_time,
        territory_id ,
        user_agent,
        resolution,
        user_id_db,
        device_id,
        api_version,
        store_id,
        local_time,
        show_recommendation,
        current_url,
        referrer_url,
        email_address,
        order_id,
        product_id,
        amount,
        price,
        currency,
        CASE 
            WHEN diamond_id IS NOT NULL OR alloy_id IS NOT NULL THEN COALESCE(diamond_id,-2) 
            ELSE 0
        END AS diamond_id,
        CASE 
            WHEN diamond_id IS NOT NULL OR alloy_id IS NOT NULL THEN COALESCE(alloy_id,-2) 
            ELSE 0
        END AS alloy_id
    FROM stg_behaviour_checkout_success_excecuted_NULL
)
SELECT 
    ROW_NUMBER() OVER() AS sale_id,
    log_id,
    date_id,
    order_time,
    territory_id,
    user_id_db,
    device_id,
    api_version,
    store_id,
    order_id,
    product_id,
    diamond_id,
    alloy_id,
    local_time,
    amount,
    price,
    currency
FROM stg_behaviour_checkout_success_check_undefine