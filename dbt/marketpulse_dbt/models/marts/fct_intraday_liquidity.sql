-- Fact: intraday liquidity profile — average volume and spread per
-- 30-minute bucket across the session. Used to answer "when is it
-- cheapest to execute?" (classic execution-desk question).

with bars as (
    select * from {{ ref('stg_ohlcv_1m') }}
)

select
    symbol,
    trade_date,
    date_trunc('hour', bar_ts)
        + interval '30 min' * floor(extract(minute from bar_ts) / 30) as bucket_ts,
    sum(volume)          as bucket_volume,
    sum(notional)        as bucket_notional,
    sum(trade_count)     as bucket_trades,
    avg(avg_spread_bps)  as avg_spread_bps,
    min(low)             as bucket_low,
    max(high)            as bucket_high
from bars
group by 1, 2, 3
