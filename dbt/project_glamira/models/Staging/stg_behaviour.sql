-- CREATE OR REPLACE FUNCTION `project-glamira.Glamira.change_price`(input STRING)
-- RETURNS STRING
-- AS (
--   CASE
--     WHEN input IS NULL THEN NULL
--     ELSE
--       -- Lấy 2 ký tự đầu
--       CONCAT(SUBSTR(input, 1, 2), '.', SUBSTR(input, -2))
--   END
-- );
WITH behaviour_source AS(
    SELECT * 
    FROM `project-glamira.Glamira.behaviour`
),
behaviour__rename AS(
    SELECT 
        behaviour._id as behaviour_key,
        behaviour.time_stamp,
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
        behaviour.recommendation,
        behaviour.utm_source,
        behaviour.utm_medium,
        behaviour.collection,
        behaviour.product_id,
        behaviour.price,
        behaviour.currency,
        behaviour.is_paypal,
        ARRAY(
            SELECT AS STRUCT
                option.alloy,
                option.diamond,
                option.shapediamond AS shape_diamond,
                option.option_label,
                option.option_id,
                option.value_label,
                option.value_id,
                option.quality,
                option.quality_label,
                option.stone,
                option.pearlcolor,
                option.finish,
                option.price,
                option.`category id` as category_id,
                option.kollektion_id,
                option.kollektion
            FROM UNNEST(option) AS option
        ) AS option,
        behaviour.order_id,
        ARRAY(
            SELECT AS STRUCT
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
                ) as option,
            FROM UNNEST(cart_products) as cart_products
        ) as cart_products,
        behaviour.recommendation_product_id,
        behaviour.recommendation_product_position,
        behaviour.recommendation_clicked_position,
        behaviour.cat_id,
        behaviour.collect_id,
        behaviour.viewing_product_id,
        behaviour.key_search
    from behaviour_source as behaviour
),
behaviour__excecuted_NULL AS(
    SELECT 
        behaviour.behaviour_key,
        behaviour.time_stamp,
        NULLIF(behaviour.ip,"") as ip,
        NULLIF(behaviour.user_agent,"") as user_agent,
        NULLIF(behaviour.resolution,"") as resolution,
        COALESCE(NULLIF(behaviour.user_id_db,""),"0") as user_id_db,
        NULLIF(behaviour.device_id,"") as device_id,
        behaviour.api_version,
        COALESCE(NULLIF(behaviour.store_id,""),"0") as store_id,
        behaviour.local_time,
        NULLIF(behaviour.show_recommendation,"") as show_recommendation,
        NULLIF(behaviour.current_url,"") as current_url,
        NULLIF(behaviour.referrer_url,"") as referrer_url,
        NULLIF(behaviour.email_address,"") as email_address,
        behaviour.recommendation,
        NULLIF(behaviour.utm_source,"") as utm_source,
        NULLIF(behaviour.utm_medium,"") as utm_medium,
        NULLIF(behaviour.collection,"") as collection,
        NULLIF(behaviour.product_id,"") as product_id,
        NULLIF(behaviour.price,"") as price,
        NULLIF(behaviour.currency,"") as currency,
        behaviour.is_paypal,
        ARRAY(
            SELECT AS STRUCT
                NULLIF(option.alloy,"") as alloy,
                NULLIF(option.diamond,"") as diamond,
                NULLIF(option.shape_diamond,"") as shape_diamond,
                NULLIF(option.option_label,"") as option_label,
                COALESCE(NULLIF(option.option_id,""),"0") as option_id,
                NULLIF(option.value_label,"") as value_label,
                COALESCE(NULLIF(option.value_id,""),"0") as value_id,
                NULLIF(option.quality,"") as quality,
                NULLIF(option.quality_label,"") as quality_label,
                NULLIF(option.stone,"") as stone,
                NULLIF(option.pearlcolor,"") as pearlcolor,
                NULLIF(option.finish,"") as finish,
                NULLIF(option.price,"") as price,
                COALESCE(NULLIF(option.category_id,""),"0") as category_id,
                COALESCE(NULLIF(option.kollektion_id,""),"0") as kollektion_id,
                NULLIF(option.kollektion,"") as kollektion
            FROM UNNEST(option) AS option
        ) AS option,
        COALESCE(NULLIF(behaviour.order_id,""),"0") as order_id,
        ARRAY(
            SELECT AS STRUCT
                cart_products.product_id ,
                cart_products.amount,
                NULLIF(cart_products.price,"") as price,
                NULLIF(cart_products.currency,"") as currency,
                ARRAY(
                    SELECT AS STRUCT
                        NULLIF(cart_products_option.option_label,"") as option_label,
                        cart_products_option.option_id,
                        NULLIF(cart_products_option.value_label,"") as value_label,
                        cart_products_option.value_id 
                    FROM UNNEST(cart_products.option) as cart_products_option
                ) as option,
            FROM UNNEST(cart_products) as cart_products
        ) as cart_products,
        COALESCE(NULLIF(behaviour.recommendation_product_id,""),"0") as recommendation_product_id,
        NULLIF(behaviour.recommendation_product_position,"") as recommendation_product_position,
        NULLIF(behaviour.recommendation_clicked_position,"") as recommendation_clicked_position,
        COALESCE(NULLIF(behaviour.cat_id,""),"0") as cat_id,
        COALESCE(NULLIF(behaviour.collect_id,""),"0") as collect_id,
        COALESCE(NULLIF(behaviour.viewing_product_id,""),"0") as viewing_product_id,
        NULLIF(behaviour.key_search,"") as key_search
    from behaviour__rename as behaviour
),
behaviour__cast_type AS(
    SELECT 
        behaviour.behaviour_key,
        behaviour.time_stamp,
        behaviour.ip,
        behaviour.user_agent,
        behaviour.resolution,
        CAST(behaviour.user_id_db AS INTEGER) as user_id_db,
        behaviour.device_id,
        CAST(behaviour.api_version AS DECIMAL) AS api_version,
        CAST(behaviour.store_id AS INTEGER) AS store_id,
        CAST(behaviour.local_time AS DATETIME) as local_time,
        behaviour.show_recommendation,
        behaviour.current_url,
        behaviour.referrer_url,
        behaviour.email_address,
        CAST(behaviour.recommendation AS BOOLEAN) AS recommendation,
        behaviour.utm_source,
        behaviour.utm_medium,
        behaviour.collection,
        CAST(behaviour.product_id AS INTEGER) as product_id,
        CAST(`project-glamira.Glamira.change_price`(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(behaviour.price,' ',''),',',''),'.',''),'\u00A0',''),"'",'')) AS NUMERIC) AS price,
        behaviour.currency,
        CAST(behaviour.is_paypal AS BOOLEAN) AS is_paypal,
        ARRAY(
            SELECT AS STRUCT
                option.alloy,
                option.diamond,
                CAST(option.shape_diamond AS INTEGER) AS shape_diamond,
                option.option_label,
                CAST(option.option_id AS INTEGER) AS option_id,
                option.value_label,
                option.value_id ,
                option.quality,
                option.quality_label,
                CAST(option.stone AS INTEGER) AS stone,
                option.pearlcolor,
                option.finish,
                CAST(`project-glamira.Glamira.change_price`(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(option.price,' ',''),',',''),'.',''),'\u00A0',''),"'",'')) AS NUMERIC) AS price,
                CAST(option.category_id AS INTEGER) AS category_id,
                CAST(option.kollektion_id AS INTEGER) AS kollektion_id,
                option.kollektion
            FROM UNNEST(option) AS option
        ) AS option,
        CAST(behaviour.order_id AS DECIMAL) AS order_id,
        ARRAY(
            SELECT AS STRUCT
                CAST(cart_products.product_id AS INTEGER) AS product_id,
                cart_products.amount,
                CAST(`project-glamira.Glamira.change_price`(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cart_products.price,' ',''),',',''),'.',''),'\u00A0',''),"'",'')) AS NUMERIC) AS price,
                cart_products.currency,
                ARRAY(
                    SELECT AS STRUCT
                        cart_products_option.option_label,
                        CAST(cart_products_option.option_id AS INTEGER) AS option_id,
                        cart_products_option.value_label,
                        CAST(cart_products_option.value_id AS INTEGER) AS value_id,
                    FROM UNNEST(cart_products.option) as cart_products_option
                ) as option,
            FROM UNNEST(cart_products) as cart_products
        ) as cart_products,
        CAST(behaviour.recommendation_product_id AS INTEGER) AS recommendation_product_id ,
        behaviour.recommendation_product_position,
        behaviour.recommendation_clicked_position,
        CAST(behaviour.cat_id AS INTEGER) AS cat_id,
        CAST(behaviour.collect_id AS INTEGER) AS collect_id,
        CAST(behaviour.viewing_product_id AS INTEGER) AS viewing_product_id,
        behaviour.key_search
    from behaviour__excecuted_NULL as behaviour   
)
SELECT *
FROM behaviour__cast_type