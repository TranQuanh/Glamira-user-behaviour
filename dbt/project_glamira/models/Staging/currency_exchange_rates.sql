SELECT
    currency,
    CASE 
        WHEN currency = 'CHF' THEN 1.04  -- Tỷ giá CHF -> USD (giá trị minh họa)
        WHEN currency = 'CAD $' THEN 0.75  -- Tỷ giá CAD -> USD
        WHEN currency = 'zł' THEN 0.25  -- Tỷ giá PLN -> USD
        WHEN currency = 'CLP' THEN 0.0013  -- Tỷ giá CLP -> USD
        WHEN currency = 'CRC ₡' THEN 0.0018  -- Tỷ giá CRC -> USD
        WHEN currency = 'NZD $' THEN 0.61  -- Tỷ giá NZD -> USD
        WHEN currency = 'Lei' THEN 0.23  -- Tỷ giá RON -> USD
        WHEN currency = 'лв.' THEN 0.56  -- Tỷ giá BGN -> USD
        WHEN currency = 'PEN S/.' THEN 0.27  -- Tỷ giá PEN -> USD
        WHEN currency = '₺' THEN 0.11  -- Tỷ giá TRY -> USD
        WHEN currency = 'GTQ Q' THEN 0.13  -- Tỷ giá GTQ -> USD
        WHEN currency = '₱' THEN 0.020  -- Tỷ giá PHP -> USD
        WHEN currency = '₫' THEN 0.000042  -- Tỷ giá VND -> USD
        WHEN currency = 'din.' THEN 1.50  -- Tỷ giá dinar -> USD (giá trị minh họa)
        WHEN currency = 'kn' THEN 0.16  -- Tỷ giá HRK -> USD
        WHEN currency = 'HKD $' THEN 0.13  -- Tỷ giá HKD -> USD
        WHEN currency = '￥' THEN 0.0070  -- Tỷ giá JPY -> USD
        WHEN currency = 'د.ك.‏' THEN 3.27  -- Tỷ giá KWD -> USD
        WHEN currency = 'USD $' THEN 1.00  -- USD -> USD
        WHEN currency = 'COP $' THEN 0.00026  -- Tỷ giá COP -> USD
        WHEN currency = '₹' THEN 0.012  -- Tỷ giá INR -> USD
        WHEN currency = 'BOB Bs' THEN 0.14  -- Tỷ giá BOB -> USD
        WHEN currency = 'UYU' THEN 0.025  -- Tỷ giá UYU -> USD
        WHEN currency = 'DOP $' THEN 0.018  -- Tỷ giá DOP -> USD
        WHEN currency = 'R$' THEN 0.20  -- Tỷ giá BRL -> USD
        WHEN currency = '₲' THEN 0.00014  -- Tỷ giá PYG -> USD
        WHEN currency = '€' THEN 1.10  -- Tỷ giá EUR -> USD 
        WHEN currency = '£' THEN 1.25  -- Tỷ giá GBP -> USD 
        WHEN currency = 'kr' THEN 0.10  -- Tỷ giá SEK -> USD 
        WHEN currency = 'AU $' THEN 0.70  -- Tỷ giá AUD -> USD 
        WHEN currency = 'SGD $' THEN 0.74  -- Tỷ giá SGD -> USD
        WHEN currency = '$' THEN 1.00  -- Tỷ giá USD -> USD
        WHEN currency = 'Kč' THEN 0.046  -- Tỷ giá CZK -> USD 
        WHEN currency = 'Ft' THEN 0.0034  -- Tỷ giá HUF -> USD 
        WHEN currency = 'Undefine' THEN NULL  -- Không xác định
        WHEN currency = 'din.' THEN 1.50  -- Tỷ giá dinar -> USD 
        WHEN currency = 'MXN $' THEN 0.054  -- Tỷ giá MXN -> USD 

        ELSE NULL  -- Hoặc bạn có thể đặt giá trị mặc định
    END AS usd_conversion_rate
FROM 
    (
        SELECT 'CHF' AS currency UNION ALL
        SELECT 'CAD $' UNION ALL
        SELECT 'zł' UNION ALL
        SELECT 'CLP' UNION ALL
        SELECT 'CRC ₡' UNION ALL
        SELECT 'NZD $' UNION ALL
        SELECT 'Lei' UNION ALL
        SELECT 'лв.' UNION ALL
        SELECT 'PEN S/.' UNION ALL
        SELECT '₺' UNION ALL
        SELECT 'GTQ Q' UNION ALL
        SELECT '₱' UNION ALL
        SELECT '₫' UNION ALL
        SELECT 'din.' UNION ALL
        SELECT 'kn' UNION ALL
        SELECT 'HKD $' UNION ALL
        SELECT '￥' UNION ALL
        SELECT 'د.ك.‏' UNION ALL
        SELECT 'USD $' UNION ALL
        SELECT 'COP $' UNION ALL
        SELECT '₹' UNION ALL
        SELECT 'BOB Bs' UNION ALL
        SELECT 'UYU' UNION ALL
        SELECT 'DOP $' UNION ALL
        SELECT 'R$' UNION ALL
        SELECT '₲' UNION ALL
        SELECT '€' UNION ALL
        SELECT '£' UNION ALL
        SELECT 'kr' UNION ALL
        SELECT 'AU $' UNION ALL
        SELECT 'SGD $' UNION ALL
        SELECT '$' UNION ALL
        SELECT 'Kč' UNION ALL
        SELECT 'Ft' UNION ALL
        SELECT 'Undefine' UNION ALL
        SELECT 'din.' UNION ALL
        SELECT 'MXN $'
    ) AS currency_list
ORDER BY 
    currency
