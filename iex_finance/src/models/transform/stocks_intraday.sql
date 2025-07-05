{% if not table_exists %}
CREATE TABLE {{ target_table }} AS
{% else %}
INSERT INTO {{ target_table }}
{% endif %}
SELECT
    stock_code,
    DATE(datetime) AS date,
    MIN(open) AS min_open,
    MAX(close) AS max_close,
    MAX(high) AS daily_high,
    MIN(low) AS daily_low,
    SUM(volume) AS daily_volume,
    SUM(numberOfTrades) AS daily_trades,
    MAX(close) - MIN(open) AS daily_return
FROM stocks_intraday
{% if table_exists %}
WHERE datetime > (
    SELECT COALESCE(MAX(date), '2000-01-01') FROM {{ target_table }}
)
{% endif %}
GROUP BY stock_code, DATE(datetime);

