-- Dimension: symbol reference data (from seed) enriched with observed
-- trading statistics. In production this would join a security master
-- (e.g. Bloomberg OpenFIGI) instead of a seed file.

with reference as (
    select * from {{ ref('symbol_reference') }}
),

observed as (
    select
        symbol,
        min(trade_date) as first_traded_date,
        max(trade_date) as last_traded_date,
        avg(realised_vol_annualised) as avg_realised_vol
    from {{ ref('stg_symbol_risk_daily') }}
    group by 1
)

select
    r.symbol,
    r.company_name,
    r.sector,
    r.base_price,
    r.annualised_vol      as design_vol,
    o.first_traded_date,
    o.last_traded_date,
    o.avg_realised_vol
from reference r
left join observed o using (symbol)
