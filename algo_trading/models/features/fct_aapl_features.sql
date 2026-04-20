with source as (
    select * from {{ ref('stg_aapl_daily') }}
),

features as (
    select
        date,
        close,

        -- Simple Moving Average 20
        avg(close) over (
            order by date
            rows between 19 preceding and current row
        ) as sma_20,

        -- EMA 12 (approximated with SMA for now)
        avg(close) over (
            order by date
            rows between 11 preceding and current row
        ) as ema_12,

        -- EMA 26 (approximated with SMA for now)
        avg(close) over (
            order by date
            rows between 25 preceding and current row
        ) as ema_26

    from source
)

select
    date,
    close,
    sma_20,
    ema_12,
    ema_26,
    ema_12 - ema_26 as macd
from features
order by date asc