"""MarketPulse — production-grade real-time market data lakehouse.

Layers:
    generator  -> synthetic-but-realistic market tick/trade event simulation
    producer   -> Kafka ingestion (exactly-once-friendly keyed publishing)
    streaming  -> Spark Structured Streaming, Kafka -> bronze (Delta Lake)
    batch      -> bronze -> silver -> gold medallion transforms
    quality    -> contract-driven data-quality engine
    warehouse  -> gold -> Postgres/DuckDB serving layer
"""

__version__ = "1.0.0"
