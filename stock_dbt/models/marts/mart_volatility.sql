{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  mart_volatility
  ────────────────
  Daily return and rolling 30-day realised volatility (std dev of daily returns)
  per ticker. High volatility → larger price swings → higher risk/reward.
*/

with prices as (

    select
        date,
        ticker,
        close,

        lag(close) over (
            partition by ticker
            order by date
        )                       as prev_close

    from {{ ref('fact_stock_prices') }}

),

daily_returns as (

    select
        date,
        ticker,
        close,
        prev_close,

        case
            when prev_close > 0
            then round(((close - prev_close) / prev_close) * 100.0, 6)
            else null
        end                     as daily_return_pct

    from prices
    where prev_close is not null

),

volatility as (

    select
        date,
        ticker,
        close,
        daily_return_pct,

        -- 30-day rolling volatility (annualised: std_dev * sqrt(252))
        round(
            stddev(daily_return_pct) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            )::numeric, 6
        )                       as volatility_30d_pct,

        round(
            (
                stddev(daily_return_pct) over (
                    partition by ticker
                    order by date
                    rows between 29 preceding and current row
                ) * sqrt(252)
            )::numeric, 6
        )                       as annualised_volatility_pct,

        -- 30-day average return
        round(
            avg(daily_return_pct) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            )::numeric, 6
        )                       as avg_return_30d_pct

    from daily_returns

)

select * from volatility
