"""CLI: python -m flux_pipeline [--config PATH] [--force] [--full-refresh]"""
from __future__ import annotations

import argparse
import logging
import sys

from .config import DEFAULT_CONFIG, load_config
from .pipeline import run


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="flux_pipeline", description=__doc__)
    p.add_argument("--config", default=DEFAULT_CONFIG, help="path to pipeline YAML")
    p.add_argument("--force", action="store_true",
                   help="reprocess batches even if already processed (proves idempotency)")
    p.add_argument("--full-refresh", action="store_true",
                   help="delete state and rebuild from all raw batches")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    return run(load_config(args.config), force=args.force, full_refresh=args.full_refresh)


if __name__ == "__main__":
    sys.exit(main())
