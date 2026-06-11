# ADR-0003: Hand-rolled data-contracts engine instead of Great Expectations

**Status:** accepted (revisit if team > 1 or checks > ~50)

## Context

We need declarative data-quality checks between every medallion layer, enforced as pipeline gates, with results persisted for observability. Options: Great Expectations, Soda Core, dbt tests only, or a small custom engine.

## Decision

A ~150-line engine (`quality/checks.py`) executes YAML contracts (`contracts/*.yml`) validated by pydantic models. Severity levels (`error` fails the task, `warn` records and continues). Results append to `ops.dq_results`.

## Rationale

- GE's API surface and runtime weight are large relative to the checks needed; its config churn is a maintenance tax.
- The contract YAML is the durable artifact. If we later adopt GE/Soda, contracts translate mechanically; the pipeline interface (a task that raises on violation) is unchanged.
- For a portfolio/learning project, demonstrating *how* DQ engines work beats demonstrating that one can configure a framework.

## Consequences

+ Zero heavy dependencies; checks run in unit tests trivially.
+ Full control of failure semantics and persistence schema.
− We own the code: new check types are our work (acceptable at current scope).
− No data-docs UI out of the box (the dashboard's DQ panel covers the need).
