-- Staging: light cleanup + renames over gold.ohlcv_1m.
-- Staging models are views: zero storage, always fresh, one place to fix
-- naming. Business logic lives in marts, not here.

with source as (
    select * from {{ source('gold', 'ohlcv_1m') }}
)

select
    symbol,
    minute              as bar_ts,
    cast(minute as date) as trade_date,
    open,
    high,
    low,
    close,
    volume,
    notional,
    vwap,
    trade_count,
    tick_count,
    avg_spread_bps
from source
