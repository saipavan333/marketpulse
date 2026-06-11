# Runbook

Operating procedures for MarketPulse. Written the way an on-call data engineer would want it: symptom → diagnosis → action.

## Daily health checks

1. Airflow: `marketpulse_medallion_hourly` green for the last 24 runs? SLA misses?
2. Dashboard DQ panel: pass rate 100%? Any `warn` trends creeping up?
3. Quarantine rate (ops DAG logs): < 1% normal (matches simulator anomaly rate ~0.5%); 1–5% investigate; > 5% the ops DAG fails by design — treat as incident.
4. MinIO/S3: bronze partitions landing for today? (`mc ls local/marketpulse-lake/bronze/events/`)

## Incident playbooks

### Streaming job dead / bronze not growing

```
docker compose ps                 # is the app container up?
docker compose logs app --tail 100
```

Most common causes: Kafka unreachable (check `kafka` healthcheck), checkpoint corruption after an unclean kill. Recovery for checkpoint corruption: stop the query, move the checkpoint dir aside, restart with `startingOffsets=earliest` — bronze writes are append-only and silver dedupes, so replaying is **safe by design**. That is why we dedupe in silver even though the producer is idempotent.

### DQ gate failed the DAG

This is the system working, not breaking. The failing check names the dataset, check, observed vs threshold (task logs + `ops.dq_results`).

1. Identify blast radius: which layer failed? Downstream tasks did not run — good, nothing is poisoned.
2. Inspect quarantine: `SELECT quarantine_reason, COUNT(*) FROM ... GROUP BY 1` (or read the parquet directly).
3. Upstream feed defect → fix/replay upstream, rerun the DAG (idempotent, safe).
4. Legitimate data drift (e.g. a new venue) → update the contract **in a PR** (contracts are reviewed like API changes), rerun.

Never weaken a contract to green a run without understanding the cause. The audit trail (`ops.dq_results`) is your history of what was tolerated when.

### Backfill a date range

```
# Airflow: clear the runs for the window; tasks are idempotent full rebuilds
airflow tasks clear marketpulse_medallion_hourly -s 2026-06-01 -e 2026-06-03
```

Re-running never double-counts: silver dedupes on business keys, gold overwrites its window, warehouse loads are delete-then-insert per table. (This idempotency is asserted by `tests/integration/test_local_pipeline_e2e.py::test_pipeline_is_idempotent`.)

### Schema change arriving from upstream

Follow `docs/data-model.md` § Schema evolution policy. Checklist: new topic version? contract updated in same PR? silver parser updated? backfill needed? consumers notified?

### Warehouse load failed mid-write

Loads run in a transaction (SQLAlchemy `engine.begin()`); a failure rolls back, the previous state stays live. Rerun the task.

### Disk filling up (local/docker)

`make destroy` removes volumes. In docker mode the daily ops DAG runs Delta `OPTIMIZE` + `VACUUM RETAIN 168 HOURS`; if you reduced retention, ensure no time-travel consumer needs older snapshots.

## Secrets & credentials

Compose ships dummy credentials for local use only. Production posture (terraform): MSK with IAM auth, S3 with KMS, RDS password in Secrets Manager, MWAA execution role with least privilege. **Rule: a leaked repo must never leak production access.**

## Useful one-liners

```bash
# peek at kafka traffic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic market.ticks.v1 --max-messages 5

# row counts straight from the lake (local mode)
python - <<'EOF'
import duckdb
print(duckdb.sql("SELECT count(*) FROM 'data/local_run/lake/silver/ticks/**/*.parquet'"))
EOF

# latest DQ results
docker compose exec postgres psql -U marketpulse -d marketpulse \
  -c "SELECT dataset, check_name, passed FROM ops.dq_results ORDER BY checked_at DESC LIMIT 20"
```
