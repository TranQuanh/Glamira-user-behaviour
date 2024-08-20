SELECT fact_order.*,
        currency_exchange.usd_conversion_rate,
        (fact_order.amount*fact_order.price*currency_exchange.usd_conversion_rate) AS total_price
FROM {{ref("stg_behaviour_checkout_success")}} fact_order
LEFT JOIN {{ref("currency_exchange_rates")}} currency_exchange ON fact_order.currency = currency_exchange.currency
