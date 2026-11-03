"""Command-line interface for configured Arrow ingestions."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from .runner import run_pipeline


def main(arguments: Sequence[str] | None = None) -> None:
    """Run the command-line interface.

    Args:
        arguments: Command-line arguments without the executable name.
    """
    parser = argparse.ArgumentParser(description="Run an Arrow ingestion.")
    commands = parser.add_subparsers(dest="command", required=True)
    run_command = commands.add_parser("run", help="Run a pipeline YAML file.")
    run_command.add_argument("config", help="Path to the pipeline YAML file.")
    args = parser.parse_args(arguments)

    if args.command == "run":
        result = run_pipeline(args.config)
        print(
            f"Wrote {result.rows_written} rows with {result.column_count} columns "
            f"to {result.destination}",
        )
