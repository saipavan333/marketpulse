"""Centralised, typed configuration.

Every setting comes from environment variables prefixed with ``MP_``
(see ``.env.example``). Defaults are tuned for **local mode** so that
``make demo`` and the test-suite run with zero services and zero setup.

Design notes (interview talking point):
    - 12-factor: config lives in the environment, never in code.
    - One typed settings object (pydantic) instead of scattered os.getenv
      calls -> typos fail fast at startup, not deep inside a Spark job.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for all MarketPulse components."""

    model_config = SettingsConfigDict(env_prefix="MP_", env_file=".env", extra="ignore")

    # --- runtime mode -------------------------------------------------------
    env: str = "local"  # "local" | "docker" | "prod"

    # --- kafka --------------------------------------------------------------
    kafka_bootstrap: str = "localhost:29092"
    topic_ticks: str = "market.ticks.v1"
    topic_trades: str = "market.trades.v1"
    topic_dlq: str = "market.dlq.v1"

    # --- lakehouse ----------------------------------------------------------
    lake_root: str = "lake"  # local dir in local mode, s3a://bucket in docker/prod
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    delta_enabled: bool = False  # parquet locally; Delta Lake in docker/prod
    checkpoint_root: str = "checkpoints"

    # --- warehouse ----------------------------------------------------------
    warehouse_url: str = "duckdb:///warehouse/marketpulse.duckdb"

    # --- spark --------------------------------------------------------------
    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = 8

    # --- generator ----------------------------------------------------------
    generator_seed: int = 42
    generator_events_per_sec: int = 200
    generator_anomaly_rate: float = 0.005

    @property
    def table_format(self) -> str:
        """Storage format for lakehouse tables."""
        return "delta" if self.delta_enabled else "parquet"

    def lake_path(self, layer: str, table: str) -> str:
        """Fully-qualified path of a lakehouse table, e.g. ``s3a://lake/bronze/ticks``."""
        return f"{self.lake_root}/{layer}/{table}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton accessor (cached so all modules share one instance)."""
    return Settings()
