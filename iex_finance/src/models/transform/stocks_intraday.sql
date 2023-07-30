{% set table_exists = engine.execute("select exists (select from pg_tables where tablename = '" + target_table + "')").first()[0] %}

{% if not table_exists %}
    drop table if exists {{ target_table }}; 
{% else %}
    {% set max_close = engine.execute("select max(max_close_value_per_day) from " + target_table).first()[0] %}
{% endif %}

{% if not table_exists %}
    create table {{ target_table }} as 
{% else %}
    insert into {{ target_table }}
{% endif %}
(
    select
        stock_code,
        max_open_value_per_day,
        min_open_value_per_day,
        max_close_value_per_day,
        min_close_value_per_day,
        max_high_per_day,
        min_high_per_day,
        max_low_per_day,
        min_low_per_day,
        status_difference,
        trades_mean,
        volume_mean
    from stocks_intraday
    {% if table_exists %}
    where 
        max_close_value_per_day > '{{ max_close }}'
    {% endif %}
); 