with source as (
    select * from {{ ref('stg_aapl_daily') }}
),
price_changes as (
    select
        date,
        close,
        close - lag(close) over (order by date) as daily_change
    from source
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
        ) as ema_26,
        -- RSI 14 components
        avg(case when daily_change > 0 then daily_change else 0 end) over (
            order by date
            rows between 13 preceding and current row
        ) as avg_gain,
        avg(case when daily_change < 0 then abs(daily_change) else 0 end) over (
            order by date
            rows between 13 preceding and current row
        ) as avg_loss
    from price_changes
)
select
    date,
    close,
    sma_20,
    ema_12,
    ema_26,
    ema_12 - ema_26 as macd,
    case when avg_loss = 0 then 100
         else 100 - (100 / (1 + (avg_gain / avg_loss)))
    end as rsi_14
from features
order by date asc