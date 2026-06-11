# ADR-0004: Pandas hop for gold → warehouse loads

**Status:** accepted (with explicit switch criteria)

## Context

Gold tables must land in the warehouse (Postgres/DuckDB) for dbt and the dashboard. Options: Spark JDBC writer, external COPY pipelines, or collect-to-pandas + SQLAlchemy.

## Decision

Gold aggregates are small by construction (≈ minutes × symbols rows, thousands not billions). `toPandas()` + SQLAlchemy in one transaction is simpler, easier to make idempotent (replace per table), and uses one code path for both Postgres and DuckDB.

## Switch criteria (when this ADR retires)

Move to Spark JDBC partitioned writes (or warehouse-native COPY from object storage) when any of: gold table > ~5M rows per load, load time > 2 minutes, or driver memory pressure observed.

## Consequences

+ Less code, transactional, trivially idempotent.
+ Same path for local DuckDB and prod Postgres.
− A driver-memory ceiling exists — documented, monitored, with a named exit strategy.
