# ADR-0001: Parquet in local mode, Delta Lake in docker/prod

**Status:** accepted

## Context

Lakehouse tables need ACID guarantees (concurrent stream + batch writers), schema enforcement, and time travel in production. But the test suite and the 60-second demo must run with `pip install` only — no JVM package downloads, no services.

## Decision

Table format is a config switch (`MP_DELTA_ENABLED`). `utils/spark.write_table/read_table` abstract the format; business logic never references it. Local/CI uses plain Parquet; docker/prod uses Delta (jars baked into the Spark image).

## Consequences

+ Tests and CI are fast and dependency-light; contributors run everything immediately.
+ The exact same transform code is exercised in both modes — what CI tests is what prod runs.
− Local mode loses ACID/time-travel; acceptable because local mode is single-writer by construction.
− Two formats means the format switch itself must be tested (covered by the e2e run in CI).

Alternatives considered: Iceberg (equivalent fit; Delta chosen for tighter Spark integration and market share in finance), Hudi (heavier operational model), "Delta everywhere" (forces jar downloads in unit tests — rejected).
