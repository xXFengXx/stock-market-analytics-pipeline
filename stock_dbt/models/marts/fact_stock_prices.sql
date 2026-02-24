{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  fact_stock_prices
  ─────────────────
  One row per (date, ticker). This is the primary fact table consumed
  by downstream marts and the BI dashboard.
*/

select
    date,
    ticker,
    open,
    high,
    low,
    close,
    adjusted_close,
    volume,

    -- Intraday price range
    round(high - low, 4)                    as intraday_range,

    -- Mid price
    round((high + low) / 2.0, 4)           as mid_price,

    -- Intraday return (pct gap between open and close)
    case
        when open > 0
        then round(((close - open) / open) * 100.0, 4)
        else null
    end                                     as intraday_return_pct

from {{ ref('stg_stock_prices') }}
