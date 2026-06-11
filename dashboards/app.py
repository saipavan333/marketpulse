"""MarketPulse analytics dashboard (Streamlit).

Reads ONLY the warehouse serving layer (never the lake) — the dashboard
is a consumer like any other, downstream of contracts and marts.

Run inside docker:  http://localhost:8501
Run locally:        MP_WAREHOUSE_URL=duckdb:///data/local_run/marketpulse.duckdb \
                    streamlit run dashboards/app.py
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

WAREHOUSE_URL = os.environ.get(
    "MP_WAREHOUSE_URL", "duckdb:///data/local_run/marketpulse.duckdb"
)

st.set_page_config(page_title="MarketPulse", page_icon="📈", layout="wide")


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    if WAREHOUSE_URL.startswith("duckdb"):
        import duckdb

        con = duckdb.connect(WAREHOUSE_URL.replace("duckdb:///", ""), read_only=True)
        try:
            return con.execute(sql).fetchdf()
        finally:
            con.close()
    from sqlalchemy import create_engine

    return pd.read_sql(sql, create_engine(WAREHOUSE_URL))


st.title("📈 MarketPulse — Market Data Platform")
st.caption("Gold-layer analytics served from the warehouse. All data is synthetic.")

try:
    risk = query("SELECT * FROM gold.symbol_risk_daily ORDER BY symbol")
    bars = query("SELECT * FROM gold.ohlcv_1m ORDER BY minute")
except Exception as exc:  # pragma: no cover
    st.error(
        "Warehouse not reachable or empty. Run the pipeline first "
        "(`make demo` locally or trigger the Airflow DAG in docker). "
        f"Details: {exc}"
    )
    st.stop()

# ----------------------------------------------------------------- KPIs ---
total_notional = float(risk["total_notional"].sum())
total_trades = int(risk["trade_count"].sum())
k1, k2, k3, k4 = st.columns(4)
k1.metric("Symbols", f"{risk['symbol'].nunique()}")
k2.metric("Total notional", f"${total_notional/1e6:,.1f}M")
k3.metric("Trades", f"{total_trades:,}")
k4.metric("Avg realised vol", f"{risk['realised_vol_annualised'].mean()*100:.1f}%")

# ------------------------------------------------------------- selector ---
symbol = st.selectbox("Symbol", sorted(bars["symbol"].unique()))
sym_bars = bars[bars["symbol"] == symbol]

# ---------------------------------------------------------- candlestick ---
left, right = st.columns([2, 1])
with left:
    st.subheader(f"{symbol} — 1-minute bars")
    fig = go.Figure(
        go.Candlestick(
            x=sym_bars["minute"],
            open=sym_bars["open"],
            high=sym_bars["high"],
            low=sym_bars["low"],
            close=sym_bars["close"],
        )
    )
    fig.update_layout(height=420, xaxis_rangeslider_visible=False, margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Volume")
    vfig = px.bar(sym_bars, x="minute", y="volume")
    vfig.update_layout(height=420, margin=dict(t=10))
    st.plotly_chart(vfig, use_container_width=True)

# ------------------------------------------------------------ risk view ---
st.subheader("Daily risk summary (all symbols)")
display = risk[
    ["symbol", "trade_date", "day_close", "day_return_pct",
     "realised_vol_annualised", "var_95_log_ret", "max_drawdown",
     "total_volume", "trade_count", "avg_spread_bps"]
].copy()
display["realised_vol_annualised"] = (display["realised_vol_annualised"] * 100).round(1)
display["max_drawdown"] = (display["max_drawdown"] * 100).round(2)
display["day_return_pct"] = display["day_return_pct"].round(2)
display.columns = ["Symbol", "Date", "Close", "Return %", "Vol % (ann.)",
                   "VaR95 (log ret)", "Max DD %", "Volume", "Trades", "Spread (bps)"]
st.dataframe(display, use_container_width=True, hide_index=True)

# ------------------------------------------------------------- dq panel ---
st.subheader("Data quality (latest contract runs)")
try:
    dq = query(
        """SELECT dataset, check_name, severity, passed, observed, threshold
           FROM ops.dq_results ORDER BY checked_at DESC LIMIT 50"""
    )
    passed = int(dq["passed"].sum())
    st.progress(passed / max(len(dq), 1), text=f"{passed}/{len(dq)} checks passed")
    st.dataframe(dq, use_container_width=True, hide_index=True)
except Exception:
    st.info("No DQ results recorded yet.")
