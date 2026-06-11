"""Gold -> warehouse serving layer (Postgres in docker/prod, DuckDB locally).

Why pandas for this hop? Gold tables are *aggregates* (thousands of rows,
not billions) — collecting them to the driver is cheap, and SQLAlchemy
gives us one code path for both Postgres and DuckDB. If gold ever grew
large we would switch to Spark's JDBC writer; see ADR-0004.

Loads are **idempotent**: full refresh per (table, trade_date) via
delete-then-insert in one transaction, so re-running a day never doubles it.
"""

from __future__ import annotations

import logging

from marketpulse.config import get_settings
from marketpulse.utils.spark import build_spark, read_table

logger = logging.getLogger(__name__)

GOLD_TABLES = ["ohlcv_1m", "symbol_risk_daily"]


def _get_engine(url: str):
    if url.startswith("duckdb"):
        # duckdb via native client (no sqlalchemy needed on this path)
        import duckdb  # noqa: F401

        path = url.replace("duckdb:///", "")
        return ("duckdb", path)

    from sqlalchemy import create_engine  # lazy: only needed for Postgres

    return ("sqlalchemy", create_engine(url))


def load_gold_tables(tables: list[str] | None = None) -> dict[str, int]:
    """Load gold lakehouse tables into the warehouse. Returns row counts."""
    settings = get_settings()
    spark = build_spark("warehouse-load")
    kind, engine = _get_engine(settings.warehouse_url)
    counts: dict[str, int] = {}

    for table in tables or GOLD_TABLES:
        pdf = read_table(spark, "gold", table).toPandas()
        if "minute" in pdf.columns:
            pdf["minute"] = pdf["minute"].astype("datetime64[us]")
        counts[table] = len(pdf)

        if kind == "duckdb":
            import os

            import duckdb

            os.makedirs(os.path.dirname(engine) or ".", exist_ok=True)
            con = duckdb.connect(engine)
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            con.register("staging_df", pdf)
            con.execute(f"CREATE OR REPLACE TABLE gold.{table} AS SELECT * FROM staging_df")
            con.close()
        else:
            with engine.begin() as conn:
                pdf.to_sql(table, conn, schema="gold", if_exists="replace", index=False)

        logger.info("Loaded gold.%s -> warehouse (%s rows)", table, counts[table])
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_gold_tables()
