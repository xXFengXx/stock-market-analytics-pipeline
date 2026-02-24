{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  mart_performance_comparison
  ────────────────────────────
  Normalises each ticker's closing price to a base of 100 on its first
  trading day, enabling apples-to-apples comparison of long-term performance
  across stocks with vastly different price levels.

  indexed_performance > 100  → stock is up vs its starting price
  indexed_performance < 100  → stock is down vs its starting price
*/

with base as (

    select
        date,
        ticker,
        close

    from {{ ref('fact_stock_prices') }}

),

first_prices as (

    select
        ticker,
        min(date)                   as first_date,
        first_value(close) over (
            partition by ticker
            order by date
            rows between unbounded preceding and unbounded following
        )                           as first_close

    from base
    group by ticker, close, date    -- need close for first_value; group removes dup

),

-- Simpler approach: use a lateral join-style CTE
anchor as (

    select
        ticker,
        close                       as anchor_close

    from (
        select
            ticker,
            close,
            row_number() over (partition by ticker order by date asc) as rn
        from base
    ) ranked
    where rn = 1

),

indexed as (

    select
        b.date,
        b.ticker,
        b.close,
        a.anchor_close,

        round(
            (b.close / nullif(a.anchor_close, 0)) * 100.0, 4
        )                           as indexed_performance,

        round(
            ((b.close - a.anchor_close) / nullif(a.anchor_close, 0)) * 100.0, 4
        )                           as cumulative_return_pct,

        rank() over (
            partition by b.date
            order by (b.close / nullif(a.anchor_close, 0)) desc
        )                           as rank_on_date

    from base b
    inner join anchor a using (ticker)

)

select * from indexed
