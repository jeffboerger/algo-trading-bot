with combined as (
    select * from {{ source('algo_trading', 'aapl_daily') }}
    union all
    select * from {{ source('algo_trading', 'msft_daily') }}
    union all
    select * from {{ source('algo_trading', 'googl_daily') }}
    union all
    select * from {{ source('algo_trading', 'nvda_daily') }}
    union all
    select * from {{ source('algo_trading', 'jpm_daily') }}
    union all
    select * from {{ source('algo_trading', 'bac_daily') }}
    union all
    select * from {{ source('algo_trading', 'dis_daily') }}
    union all
    select * from {{ source('algo_trading', 'amzn_daily') }}
    union all
    select * from {{ source('algo_trading', 'wmt_daily') }}
    union all
    select * from {{ source('algo_trading', 'xom_daily') }}
    union all
    select * from {{ source('algo_trading', 'jnj_daily') }}
    union all
    select * from {{ source('algo_trading', 'tsla_daily') }}
    union all
    select * from {{ source('algo_trading', 'amd_daily') }}
),
price_changes as (
    select
        date,
        ticker,
        close,
        close - lag(close) over (partition by ticker order by date) as daily_change
    from combined
),
features as (
    select
        date,
        ticker,
        close,
        avg(close) over (
            partition by ticker
            order by date
            rows between 19 preceding and current row
        ) as sma_20,
        avg(close) over (
            partition by ticker
            order by date
            rows between 11 preceding and current row
        ) as ema_12,
        avg(close) over (
            partition by ticker
            order by date
            rows between 25 preceding and current row
        ) as ema_26,
        avg(case when daily_change > 0 then daily_change else 0 end) over (
            partition by ticker
            order by date
            rows between 13 preceding and current row
        ) as avg_gain,
        avg(case when daily_change < 0 then abs(daily_change) else 0 end) over (
            partition by ticker
            order by date
            rows between 13 preceding and current row
        ) as avg_loss
    from price_changes
)
select
    date,
    ticker,
    close,
    sma_20,
    ema_12,
    ema_26,
    ema_12 - ema_26 as macd,
    case when avg_loss = 0 then 100
         else 100 - (100 / (1 + (avg_gain / avg_loss)))
    end as rsi_14
from features
order by ticker, date asc