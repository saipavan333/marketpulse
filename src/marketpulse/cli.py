"""MarketPulse command-line interface.

Examples:
    marketpulse produce --duration 600        # stream events into Kafka
    marketpulse stream                        # Kafka -> bronze (runs forever)
    marketpulse silver                        # bronze -> silver batch
    marketpulse gold                          # silver -> gold batch
    marketpulse dq --contract contracts/silver_ticks.yml --layer silver --table ticks
    marketpulse load-warehouse                # gold -> Postgres/DuckDB
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(name="marketpulse", help="MarketPulse data platform CLI", no_args_is_help=True)
console = Console()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@app.command()
def produce(
    duration: int = typer.Option(300, help="Seconds to produce for"),
    events_per_sec: int = typer.Option(None, help="Override events/sec"),
) -> None:
    """Stream simulated market events into Kafka."""
    from marketpulse.producer import MarketDataProducer

    delivered = MarketDataProducer().run(duration_seconds=duration, events_per_sec=events_per_sec)
    console.print(f"[green]Delivered {delivered:,} events[/green]")


@app.command()
def stream() -> None:
    """Run the Kafka -> bronze Structured Streaming job (blocks)."""
    from marketpulse.streaming.bronze_stream import start_bronze_stream

    start_bronze_stream()


@app.command()
def silver() -> None:
    """Run the bronze -> silver batch transform."""
    from marketpulse.batch import silver as job

    _print_counts("silver", job.run())


@app.command()
def gold() -> None:
    """Run the silver -> gold batch transform."""
    from marketpulse.batch import gold as job

    _print_counts("gold", job.run())


@app.command()
def dq(
    contract: str = typer.Option(..., help="Path to contract YAML"),
    layer: str = typer.Option(..., help="Lakehouse layer, e.g. silver"),
    table: str = typer.Option(..., help="Table name, e.g. ticks"),
) -> None:
    """Run a data contract against a lakehouse table; non-zero exit on failure."""
    from marketpulse.quality import load_contract, run_contract
    from marketpulse.quality.checks import enforce, persist_results
    from marketpulse.utils.spark import build_spark, read_table

    spark = build_spark("dq")
    df = read_table(spark, layer, table)
    results = run_contract(df, load_contract(contract))
    run_id = persist_results(results)
    console.print(f"DQ run [bold]{run_id}[/bold]: "
                  f"{sum(r.passed for r in results)}/{len(results)} checks passed")
    enforce(results)  # raises -> exit 1 if error-severity failures


@app.command(name="load-warehouse")
def load_warehouse() -> None:
    """Load gold tables into the warehouse serving layer."""
    from marketpulse.batch.warehouse import load_gold_tables

    _print_counts("warehouse", load_gold_tables())


def _print_counts(stage: str, counts: dict[str, int]) -> None:
    table = Table(title=f"{stage} results")
    table.add_column("table")
    table.add_column("rows", justify="right")
    for name, n in counts.items():
        table.add_row(name, f"{n:,}")
    console.print(table)


if __name__ == "__main__":
    app()
