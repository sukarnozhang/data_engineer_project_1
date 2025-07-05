-- Check if the target table already exists in the database
{% set table_exists = engine.execute(
    "select exists (select from pg_tables where tablename = '" + target_table + "')"
).first()[0] %} ---example : row = (tableA,) value = row[0]  # tableA

-- Determine whether to create a new table or insert into an existing one
{% if not table_exists %}
    create table {{ target_table }} as
{% else %}
    insert into {{ target_table }}
{% endif %}
(
    -- Select and aggregate daily stock data
    select
        stock_code,                             -- Unique identifier for the stock
        date(datetime) as date,                 -- Extract the date part from datetime
        min(open) as min_open,                 -- First open price of the day
        max(close) as max_close,             -- Last close price of the day
        max(high) as daily_high,                -- Highest price of the day
        min(low) as daily_low,                 -- Lowest price of the day
        sum(volume) as daily_volume,            -- Total volume traded that day
        sum(numberOfTrades) as daily_trades,    -- Total number of trades that day
        max(close) - min(open) as daily_return  -- Net price change across the day
    from stocks_intraday

    -- If the table exists, only add newer data (incremental load), add an arbitrarily old date just in case null
    {% if table_exists %}
    where datetime > (
        select coalesce(max(date), '2000-01-01') from {{ target_table }}
    )
    {% endif %}

    group by stock_code, date(datetime)         -- Group by stock and day
);
