WITH raw_data AS (
    SELECT * FROM {{ ref('stock_prices')}}
)

SELECT  
    symbol, 
    date, 
    close,
    volume, 
    -- Calculate 7-day moving average
    AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS stock_7d_moving_avg
FROM raw_data
ORDER BY symbol, date DESC
