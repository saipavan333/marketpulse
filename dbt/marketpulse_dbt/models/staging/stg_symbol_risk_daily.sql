-- Staging: gold.symbol_risk_daily, typed and renamed.

with source as (
    select * from {{ source('gold', 'symbol_risk_daily') }}
)

select
    symbol,
    trade_date,
    day_open,
    day_high,
    day_low,
    day_close,
    day_return_pct,
    total_volume,
    total_notional,
    trade_count,
    realised_vol_annualised,
    var_95_log_ret,
    max_drawdown,
    avg_spread_bps
from source
