SELECT
    symbol,
    date, 
    close, 
    volume
FROM {{ source('raw_data', 'lab2_stock_info') }}