-- analyses/exploratory_top_performers.sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Ad-hoc query: find which tickers have outperformed over the last 1-year
-- window based on indexed performance.
-- Run with: dbt compile --select exploratory_top_performers
-- ──────────────────────────────────────────────────────────────────────────────

with latest as (
    select
        ticker,
        indexed_performance,
        cumulative_return_pct,
        rank_on_date,
        date
    from {{ ref('mart_performance_comparison') }}
    where date = (select max(date) from {{ ref('mart_performance_comparison') }})
)

select
    ticker,
    round(indexed_performance, 2)       as indexed_performance,
    round(cumulative_return_pct, 2)     as total_return_pct,
    rank_on_date                        as performance_rank
from latest
order by indexed_performance desc
