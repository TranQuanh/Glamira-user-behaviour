WITH stg_behaviour_checkout_success_source AS(
    SELECT
        ROW_NUMBER() OVER() AS id,
        behaviour.behaviour_key,
        behaviour.ip,
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
        ARRAY(
            SELECT AS STRUCT
                cart_products_option.option_label,
                cart_products_option.option_id,
                cart_products_option.value_label,
                cart_products_option.value_id
            FROM UNNEST(cart_products.option) as cart_products_option
        ) as option
    FROM {{ref("stg_behaviour")}} as behaviour,
    UNNEST(cart_products) as cart_products
    where collection = "checkout_success"
)
SELECT *
FROM stg_behaviour_checkout_success_source