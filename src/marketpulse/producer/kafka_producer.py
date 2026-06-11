"""Kafka producer: simulator events -> versioned topics.

Reliability decisions (interview talking points):
    - ``acks=all`` + idempotence: broker-side dedupe of producer retries,
      so transient network errors cannot create duplicates *from us*.
      (Upstream systems still can — which is why silver dedupes too:
      defence in depth.)
    - Keyed by symbol: per-symbol ordering within a partition, which the
      streaming layer's watermark logic relies on.
    - Delivery callbacks count failures; the job exits non-zero if any
      message ultimately fails -> orchestrator sees a red task, not silence.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from marketpulse.config import get_settings
from marketpulse.generator import MarketSimulator, SimulatorConfig

logger = logging.getLogger(__name__)


class MarketDataProducer:
    """Streams simulator events into Kafka topics (ticks + trades)."""

    def __init__(self, bootstrap: str | None = None) -> None:
        settings = get_settings()
        self.bootstrap = bootstrap or settings.kafka_bootstrap
        self.topic_ticks = settings.topic_ticks
        self.topic_trades = settings.topic_trades
        self._failed = 0
        self._delivered = 0
        # Lazy import so unit tests / local mode never need librdkafka.
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "confluent-kafka is not installed. Install with: pip install '.[kafka]'"
            ) from exc
        self._producer = Producer(
            {
                "bootstrap.servers": self.bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 20,
                "batch.num.messages": 1000,
            }
        )

    # ------------------------------------------------------------------ api
    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._failed += 1
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            self._delivered += 1

    def run(self, duration_seconds: int = 300, events_per_sec: int | None = None) -> int:
        """Produce events for ``duration_seconds``. Returns count delivered."""
        settings = get_settings()
        eps = events_per_sec or settings.generator_events_per_sec
        sim = MarketSimulator(
            SimulatorConfig(
                seed=settings.generator_seed, anomaly_rate=settings.generator_anomaly_rate
            )
        )
        logger.info("Producing ~%s events/sec for %ss to %s", eps, duration_seconds, self.bootstrap)
        deadline = time.monotonic() + duration_seconds
        while time.monotonic() < deadline:
            batch_start = time.monotonic()
            for event in sim.stream(eps):
                topic = self.topic_trades if event["event_type"] == "trade" else self.topic_ticks
                self._producer.produce(
                    topic=topic,
                    key=event["symbol"].encode(),
                    value=json.dumps(event).encode(),
                    on_delivery=self._on_delivery,
                )
            self._producer.poll(0)
            # pace to ~1 second per batch
            sleep_for = 1.0 - (time.monotonic() - batch_start)
            if sleep_for > 0:
                time.sleep(sleep_for)
        remaining = self._producer.flush(30)
        if remaining or self._failed:
            raise RuntimeError(
                f"Producer finished dirty: {self._failed} failed, {remaining} unflushed"
            )
        logger.info("Delivered %s events cleanly", self._delivered)
        return self._delivered
