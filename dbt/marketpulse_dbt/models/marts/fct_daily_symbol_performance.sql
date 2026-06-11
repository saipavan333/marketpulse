-- Fact: one row per symbol per trading day — the table an analyst or
-- risk manager actually queries. Adds sector context and liquidity rank.

with risk as (
    select * from {{ ref('stg_symbol_risk_daily') }}
),

symbols as (
    select * from {{ ref('dim_symbol') }}
)

select
    r.symbol,
    s.company_name,
    s.sector,
    r.trade_date,
    r.day_open,
    r.day_high,
    r.day_low,
    r.day_close,
    r.day_return_pct,
    r.total_volume,
    r.total_notional,
    r.trade_count,
    r.realised_vol_annualised,
    r.var_95_log_ret,
    r.max_drawdown,
    r.avg_spread_bps,
    rank() over (
        partition by r.trade_date
        order by r.total_notional desc
    ) as liquidity_rank,
    case
        when r.realised_vol_annualised >= 0.50 then 'HIGH'
        when r.realised_vol_annualised >= 0.25 then 'MEDIUM'
        else 'LOW'
    end as vol_bucket
from risk r
left join symbols s using (symbol)
