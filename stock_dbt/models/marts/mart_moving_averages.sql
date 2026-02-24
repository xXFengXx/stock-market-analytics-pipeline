{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  mart_moving_averages
  ─────────────────────
  7-day and 30-day simple moving averages of the closing price,
  computed per ticker using window functions.
*/

with base as (

    select
        date,
        ticker,
        close

    from {{ ref('fact_stock_prices') }}

),

moving_avgs as (

    select
        date,
        ticker,
        close,

        -- 7-day SMA
        round(
            avg(close) over (
                partition by ticker
                order by date
                rows between 6 preceding and current row
            ), 4
        )                       as sma_7d,

        -- 30-day SMA
        round(
            avg(close) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            ), 4
        )                       as sma_30d,

        -- 90-day SMA
        round(
            avg(close) over (
                partition by ticker
                order by date
                rows between 89 preceding and current row
            ), 4
        )                       as sma_90d,

        -- Number of rows available in the window (useful for filtering warm-up periods)
        count(close) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        )                       as window_30d_rows

    from base

)

select * from moving_avgs
