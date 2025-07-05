-- weekly_stock_agg.sql

-- Create the target table if it doesn't exist
-- This table stores aggregated weekly stock data
CREATE TABLE IF NOT EXISTS {{ target_table }} (
    stock_code VARCHAR NOT NULL,           -- Stock ticker symbol
    week_start DATE NOT NULL,              -- Start date of the week (Monday by default)
    min_open FLOAT,                        -- Minimum opening price in the week
    max_close FLOAT,                       -- Maximum closing price in the week
    weekly_high FLOAT,                     -- Highest price reached during the week
    weekly_low FLOAT,                      -- Lowest price reached during the week
    weekly_volume FLOAT,                   -- Total volume traded in the week
    weekly_trades FLOAT,                   -- Total number of trades executed in the week
    weekly_return FLOAT,                   -- Weekly return (max close - min open)
    PRIMARY KEY (stock_code, week_start)  -- Composite primary key for uniqueness
);

-- CTE to find the last week that was already loaded in the target table
WITH last_load AS (
    SELECT COALESCE(MAX(week_start), '2000-01-01') AS last_week  -- Defaults to an old date if empty
    FROM {{ target_table }}
)

-- Insert new weekly aggregated data for weeks after the last loaded week
INSERT INTO {{ target_table }} (
    stock_code,
    week_start,
    min_open,
    max_close,
    weekly_high,
    weekly_low,
    weekly_volume,
    weekly_trades,
    weekly_return
)
SELECT
    stock_code,
    DATE_TRUNC('week', datetime)::date AS week_start,  -- Truncate datetime to the week start date
    MIN(open) AS min_open,                             -- Minimum open price for the week
    MAX(close) AS max_close,                           -- Maximum close price for the week
    MAX(high) AS weekly_high,                          -- Max high price for the week
    MIN(low) AS weekly_low,                            -- Min low price for the week
    SUM(volume) AS weekly_volume,                      -- Sum of volume traded in the week
    SUM(numberOfTrades) AS weekly_trades,              -- Sum of trades executed in the week
    MAX(close) - MIN(open) AS weekly_return            -- Weekly return: price change over the week
FROM stocks_intraday
-- Only aggregate data for weeks after the last week already processed
WHERE DATE_TRUNC('week', datetime)::date > (SELECT last_week FROM last_load)
GROUP BY stock_code, DATE_TRUNC('week', datetime)::date;
