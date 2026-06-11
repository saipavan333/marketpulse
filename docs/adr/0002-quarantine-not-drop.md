# ADR-0002: Quarantine invalid rows; never filter silently

**Status:** accepted

## Context

Silver must reject invalid rows (null prices, crossed quotes, negative quantities). The obvious `df.filter(valid)` makes bad rows vanish — undetectable, unauditable, and in a financial context potentially a compliance failure.

## Decision

`split_quarantine` routes every invalid row to a `quarantine_*` table with a `quarantine_reason` column. Conservation law: parsed = clean + quarantined (tested). The daily ops DAG reports quarantine rates and fails above 5%.

## Consequences

+ Every rejected record is preserved with evidence — auditors and incident reviews get answers.
+ Quarantine rate becomes a monitorable signal of upstream health.
+ Re-processing after an upstream fix is possible (the data still exists).
− Slightly more storage and one more table per dataset — trivial cost.
